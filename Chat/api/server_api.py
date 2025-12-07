import grpc
from concurrent import futures
import time
import uuid
import json
import queue
import hashlib
import threading

import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import chat_pb2
import chat_pb2_grpc
from google.protobuf import empty_pb2

from auth.jwt_handler import create_token, decode_token
from storage.cassandra_store import CassandraStore
from broker.kafka_broker import KafkaBroker

class ChatService(chat_pb2_grpc.ChatServicer):

    def __init__(self):
        self.store = CassandraStore()
        self.broker = KafkaBroker()

        self.online_users = set()
        self._online_lock = threading.Lock()

        print("[API] ChatService inicializado.")

    #  Autenticação
    def _get_username_from_context(self, context):
        md = dict(context.invocation_metadata())
        token = md.get("authorization", "")
        username = decode_token(token)
        if not username:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token inválido ou ausente")
        return username

    #  LOGIN
    def Login(self, request, context):
        username = request.username.strip()
        password = request.password.strip()

        if not username or not password:
            return chat_pb2.LoginResponse(
                ok=False, token="", message="Usuário e senha obrigatórios."
            )

        pwd_hash = hashlib.sha256(password.encode()).hexdigest()

        stored_hash = self.store.get_user_password_hash(username)

        if stored_hash is None:
            self.store.create_user_if_not_exists(username, pwd_hash)
            print(f"[API] Usuário novo criado: {username}")
        else:
            if stored_hash != pwd_hash:
                print(f"[API] Login falhou para {username}: senha inválida")
                return chat_pb2.LoginResponse(
                    ok=False, token="", message="Senha inválida."
                )

        token = create_token(username)

        print(f"[API] Login bem-sucedido para {username}")
        return chat_pb2.LoginResponse(ok=True, token=token, message="Login OK.")

    #  STREAM DE MENSAGENS
    def StreamMessages(self, request, context):
        md = dict(context.invocation_metadata())
        print("[API] METADATA RECEBIDO NO STREAM:", md)

        username = self._get_username_from_context(context)

        event_queue: "queue.Queue[chat_pb2.Event]" = queue.Queue()

        def on_event(data: dict):
            print(f"[API] on_event ({username}) recebeu:", data)

            kind = data.get("kind")

            if kind == "private":
                to = data.get("to") or []
                sender = data.get("from")
                if username not in to and username != sender:
                    return

            if kind == "group":
                group_name = (data.get("group") or "").strip()
                if not self.store.is_user_in_group(username, group_name):
                    return

            ev = chat_pb2.Event()

            if "system" in data:
                ev.system.content = data["system"]["content"]

            elif kind == "private":
                pm = ev.private_msg
                pm.sender = data["from"]
                pm.target = data["to"][0]
                pm.content = data["payload"]["text"]

            elif kind == "group":
                gm = ev.group_msg
                gm.sender = data["from"]
                gm.group = data["group"]
                gm.content = data["payload"]["text"]

            event_queue.put(ev)

        with self._online_lock:
            self.online_users.add(username)

        self.broker.subscribe_user(username, on_event)
        print(f"[API] StreamMessages iniciado para {username}")

        try:
            while True:
                ev = event_queue.get()
                yield ev

        except Exception as e:
            print(f"[API] Erro no StreamMessages de {username}: {e!r}")

        finally:
            with self._online_lock:
                self.online_users.discard(username)

            self.broker.broadcast_system(f"{username} ficou offline")

    #  ENVIO DE MENSAGEM
    def SendMessage(self, request, context):
        sender = self._get_username_from_context(context)

        targets = list(request.to)
        if not targets:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Pelo menos um destinatário é obrigatório",
            )

        target = targets[0]

        is_group = self.store.is_group(target)

        if is_group and not self.store.is_user_in_group(sender, target):
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"Você não participa do grupo '{target}'",
            )

        msg_id = request.message_id or str(uuid.uuid4())
        conv_id = request.conversation_id or msg_id

        payload = {
            "type": request.payload.type or "text",
            "text": request.payload.text,
            "file_id": request.payload.file_id,
        }

        event = {
            "message_id": msg_id,
            "conversation_id": conv_id,
            "from": sender,
            "to": targets,
            "payload": payload,
            "channels": list(request.channels),
            "kind": "group" if is_group else "private",
            "group": target if is_group else "",
            "ts_unix_ms": int(time.time() * 1000),
        }

        event["status"] = "SENT"
        self.store.save_message(event)
        self.broker.publish_incoming(event)

        print(f"[API] Mensagem enviada por {sender} → {targets}")
        return chat_pb2.Ack(ok=True, message="Mensagem aceita.")

    #  GRUPOS
    def CreateGroup(self, request, context):
        username = self._get_username_from_context(context)
        group = request.group.strip()

        if not group:
            return chat_pb2.Ack(ok=False, message="Nome inválido.")

        self.store.create_group(group)
        self.store.add_user_to_group(username, group)

        print(f"[API] Grupo criado: {group}")
        return chat_pb2.Ack(ok=True, message="Grupo criado.")

    def JoinGroup(self, request, context):
        username = self._get_username_from_context(context)
        group = request.group.strip()

        if not group:
            return chat_pb2.Ack(ok=False, message="Nome do grupo inválido.")

        self.store.add_user_to_group(username, group)

        print(f"[API] {username} entrou no grupo {group}")
        return chat_pb2.Ack(ok=True, message="Entrou no grupo.")

    def ListGroups(self, request, context):
        groups = self.store.list_groups()
        resp = chat_pb2.GroupListResponse()
        resp.groups.extend(groups)
        return resp

    #  LISTA DE ONLINE
    def ListOnlineUsers(self, request, context):
        resp = chat_pb2.OnlineUsersResponse()
        with self._online_lock:
            resp.usernames.extend(list(self.online_users))
        return resp

#  SERVIDOR GRPC
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServicer_to_server(ChatService(), server)
    server.add_insecure_port("[::]:50051")
    print("[API] Servidor gRPC iniciado na porta 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
