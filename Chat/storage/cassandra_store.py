import logging
from typing import List, Dict, Any, Optional

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[CassandraStore] %(asctime)s %(levelname)s: %(message)s",
)

class CassandraStore:
    """
    Camada de acesso ao Cassandra para:
      - salvar mensagens
      - atualizar status (SENT / DELIVERED / READ)
      - consultar histórico por conversa
      - gerenciar grupos (criar, adicionar usuário, listar)
    """

    def __init__(self, hosts: Optional[list] = None, keyspace: str = "chat4all"):
        if hosts is None:
            hosts = ["127.0.0.1"]

        self.cluster = Cluster(hosts)
        logger.info("Conectando ao Cassandra em %s, keyspace=%s", hosts, keyspace)
        self.session = self.cluster.connect(keyspace)

        self._prepare_statements()

    # USUÁRIOS
    def create_user_if_not_exists(self, username: str, pwd_hash: str):
        """
        Cria um usuário na tabela users apenas se ainda não existir.
        """
        username = username.strip()
        if not username:
            return

        result = self.session.execute(
            self._stmt_insert_user_if_not_exists,
            (username, pwd_hash),
        )

        applied = result[0].applied
        if applied:
            logger.info("Usuário criado: %s", username)
        else:
            logger.info("Usuário já existia: %s", username)

    # PREPARED STATEMENTS
    def _prepare_statements(self):
        # INSERT de mensagem na tabela principal
        self._stmt_insert_message_conv = self.session.prepare(
            """
            INSERT INTO messages_by_conversation (
                conversation_id,
                ts_unix_ms,
                message_id,
                sender,
                recipients,
                channels,
                payload_type,
                payload_text,
                payload_file_id,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # INDEX auxiliar messages_by_id
        self._stmt_insert_message_id = self.session.prepare(
            """
            INSERT INTO messages_by_id (
                message_id,
                conversation_id,
                sender,
                status
            )
            VALUES (?, ?, ?, ?)
            """
        )

        # SELECT das últimas mensagens de uma conversa
        self._stmt_select_messages_by_conversation = self.session.prepare(
            """
            SELECT conversation_id,
                   ts_unix_ms,
                   message_id,
                   sender,
                   recipients,
                   channels,
                   payload_type,
                   payload_text,
                   payload_file_id,
                   status
            FROM messages_by_conversation
            WHERE conversation_id = ?
            ORDER BY ts_unix_ms DESC
            LIMIT ?
            """
        )

        # UPDATE de status na tabela de mensagens
        self._stmt_update_status_conv = self.session.prepare(
            """
            UPDATE messages_by_conversation
            SET status = ?
            WHERE conversation_id = ?
              AND ts_unix_ms = ?
              AND message_id = ?
            """
        )

        # UPDATE de status na tabela messages_by_id
        self._stmt_update_status_id = self.session.prepare(
            """
            UPDATE messages_by_id
            SET status = ?
            WHERE message_id = ?
            """
        )

        # USERS
        self._stmt_insert_user_if_not_exists = self.session.prepare(
            """
            INSERT INTO users (username, password_hash)
            VALUES (?, ?)
            IF NOT EXISTS
            """
        )

        self._stmt_select_user_by_username = self.session.prepare(
            """
            SELECT username, password_hash
            FROM users
            WHERE username = ?
            """
        )

        # grupos
        self._stmt_insert_group = self.session.prepare(
            "INSERT INTO groups (name) VALUES (?)"
        )

        self._stmt_select_group_by_name = self.session.prepare(
            "SELECT name FROM groups WHERE name = ?"
        )

        self._stmt_insert_group_member = self.session.prepare(
            """
            INSERT INTO group_members (group_name, username)
            VALUES (?, ?)
            """
        )

        self._stmt_select_groups = SimpleStatement("SELECT name FROM groups")

        logger.info("Prepared statements inicializados.")

    def get_user_password_hash(self, username: str) -> Optional[str]:
        """
        Retorna o password_hash do usuário ou None se não existir.
        """
        username = username.strip()
        if not username:
            return None

        row = self.session.execute(
            self._stmt_select_user_by_username,
            (username,),
        ).one()

        if row is None:
            return None

        return row.password_hash

    # MENSAGENS
    def save_message(self, event: Dict[str, Any]):
        """
        Salva uma mensagem nas tabelas:
          - messages_by_conversation
          - messages_by_id

        Espera um dict 'event' no formato:
          {
            "conversation_id": str,
            "ts_unix_ms": int,
            "message_id": str,
            "from": str,
            "to": [str],
            "channels": [str],
            "payload": {
                "type": "text" / "file",
                "text": str,
                "file_id": str
            },
            "status": "SENT" / "DELIVERED" / "READ"   (opcional)
          }
        """
        conversation_id = event.get("conversation_id")
        ts_unix_ms = event.get("ts_unix_ms")
        message_id = event.get("message_id")
        sender = event.get("from")
        recipients = event.get("to", []) or []
        channels = event.get("channels", []) or []
        payload = event.get("payload", {}) or {}
        payload_type = payload.get("type", "text")
        payload_text = payload.get("text", "")
        payload_file_id = payload.get("file_id", "")
        status = event.get("status", "SENT")

        if not conversation_id or not message_id or ts_unix_ms is None:
            logger.warning(
                "Tentativa de salvar mensagem sem chaves suficientes: %s", event
            )
            return

        # tabela principal
        self.session.execute(
            self._stmt_insert_message_conv,
            (
                conversation_id,
                ts_unix_ms,
                message_id,
                sender,
                recipients,
                channels,
                payload_type,
                payload_text,
                payload_file_id,
                status,
            ),
        )

        # índice auxiliar por message_id
        self.session.execute(
            self._stmt_insert_message_id,
            (
                message_id,
                conversation_id,
                sender,
                status,
            ),
        )

        logger.info(
            "Mensagem salva: conv=%s ts=%s msg_id=%s status=%s",
            conversation_id,
            ts_unix_ms,
            message_id,
            status,
        )

    def update_message_status(
        self,
        conversation_id: str,
        ts_unix_ms: int,
        message_id: str,
        status: str,
    ):
        """
        Atualiza status em:
          - messages_by_conversation
          - messages_by_id
        """
        self.session.execute(
            self._stmt_update_status_conv,
            (status, conversation_id, ts_unix_ms, message_id),
        )
        self.session.execute(
            self._stmt_update_status_id,
            (status, message_id),
        )
        logger.info(
            "Status atualizado: conv=%s ts=%s msg_id=%s => %s",
            conversation_id,
            ts_unix_ms,
            message_id,
            status,
        )

    def get_messages_by_conversation(
        self, conversation_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Busca as últimas 'limit' mensagens de uma conversa.
        """
        rows = self.session.execute(
            self._stmt_select_messages_by_conversation,
            (conversation_id, limit),
        )

        result = []
        for r in rows:
            result.append(
                {
                    "conversation_id": r.conversation_id,
                    "ts_unix_ms": r.ts_unix_ms,
                    "message_id": r.message_id,
                    "sender": r.sender,
                    "recipients": list(r.recipients or []),
                    "channels": list(r.channels or []),
                    "payload_type": r.payload_type,
                    "payload_text": r.payload_text,
                    "payload_file_id": r.payload_file_id,
                    "status": r.status,
                }
            )
        return result

    # GRUPOS
    def create_group(self, group_name: str):
        """
        Cria um grupo na tabela 'groups' (coluna name).
        """
        group_name = group_name.strip()
        if not group_name:
            return

        self.session.execute(self._stmt_insert_group, (group_name,))
        logger.info("Grupo criado: %s", group_name)

    def add_user_to_group(self, username: str, group_name: str):
        """
        Adiciona usuário à tabela group_members.
        """
        username = username.strip()
        group_name = group_name.strip()
        if not username or not group_name:
            return

        self.session.execute(
            self._stmt_insert_group_member,
            (group_name, username),
        )
        logger.info("Usuário %s adicionado ao grupo %s", username, group_name)

    def get_group_members(self, group_name: str) -> List[str]:
        """
        Retorna a lista de usernames que pertencem a um determinado grupo.
        """
        group_name = group_name.strip()
        if not group_name:
            return []

        rows = self.session.execute(
            "SELECT username FROM group_members WHERE group_name = %s",
            (group_name,),
        )
        members = [r.username for r in rows]
        logger.info("Membros do grupo %s: %s", group_name, members)
        return members
    
    def is_group(self, group_name: str) -> bool:
        """
        Retorna True se o grupo existir na tabela groups.
        """
        group_name = group_name.strip()
        if not group_name:
            return False

        row = self.session.execute(
            "SELECT name FROM groups WHERE name = %s",
            (group_name,),
        ).one()
        return row is not None

    def is_user_in_group(self, username: str, group_name: str) -> bool:
        """
        Retorna True se o usuário for membro do grupo.
        """
        username = username.strip()
        group_name = group_name.strip()
        if not username or not group_name:
            return False

        row = self.session.execute(
            """
            SELECT username
            FROM group_members
            WHERE group_name = %s AND username = %s
            """,
            (group_name, username),
        ).one()
        return row is not None


    def list_groups(self) -> List[str]:
        """
        Retorna uma lista com os nomes de todos os grupos.
        """
        rows = self.session.execute(self._stmt_select_groups)
        grupos = [r.name for r in rows]
        logger.info("list_groups -> %s", grupos)
        return grupos
    
    # FECHAMENTO
    def shutdown(self):
        logger.info("Encerrando CassandraStore...")
        self.cluster.shutdown()
