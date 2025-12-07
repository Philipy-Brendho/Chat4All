import os
import uuid
import hashlib
import logging
import json
from datetime import datetime, timezone
from typing import BinaryIO, Optional, Tuple

from minio import Minio
from minio.error import S3Error
from io import BytesIO

from storage.cassandra_store import CassandraStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class FileStore:
    """
    FileStore usando Object Storage (MinIO / S3-compatible).

    Responsabilidades:
    - Upload simples (um arquivo só).
    - Upload multipart/chunked (resumable) usando diretório temporário.
    - Registrar metadados no Cassandra (tabela files).
    """

    def __init__(self, base_dir: str = "files_tmp", multipart_dir: str = "uploads_tmp"):

        self.base_dir = base_dir
        self.multipart_dir = multipart_dir
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.multipart_dir, exist_ok=True)

        self.store = CassandraStore()

        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        bucket_name = os.getenv("MINIO_BUCKET", "chat4all-files")

        self.bucket_name = bucket_name

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        found = self.client.bucket_exists(self.bucket_name)
        if not found:
            logger.info(f"Criando bucket {self.bucket_name} no MinIO...")
            self.client.make_bucket(self.bucket_name)
        else:
            logger.info(f"Bucket {self.bucket_name} já existe no MinIO.")

        logger.info("FileStore (Object Storage + multipart) inicializado.")

    # Helpers internos
    def _generate_uuid(self) -> str:
        return str(uuid.uuid4())

    def _compute_checksum_and_size(self, data: bytes) -> Tuple[str, int]:
        md = hashlib.sha256()
        md.update(data)
        checksum = md.hexdigest()
        size_bytes = len(data)
        return checksum, size_bytes

    def _temp_path(self, file_id: str) -> str:
        subdir1 = file_id[0:2]
        subdir2 = file_id[2:4]
        path_dir = os.path.join(self.base_dir, subdir1, subdir2)
        os.makedirs(path_dir, exist_ok=True)
        return os.path.join(path_dir, f"{file_id}.bin")

    def _multipart_path(self, upload_id: str) -> str:
        return os.path.join(self.multipart_dir, upload_id)

    def _multipart_meta_path(self, upload_id: str) -> str:
        return os.path.join(self._multipart_path(upload_id), "meta.json")

    # Upload "simples" (não chunked)
    def save_file(
        self,
        uploader: str,
        conversation_id: str,
        filename: str,
        mime_type: str,
        fobj: BinaryIO,
    ) -> str:
        """
        Upload simples (não multipart). Ainda é útil para arquivos menores.
        """

        file_id = self._generate_uuid()
        raw_bytes = fobj.read()
        if isinstance(raw_bytes, str):
            raw_bytes = raw_bytes.encode("utf-8")

        checksum, size_bytes = self._compute_checksum_and_size(raw_bytes)

        # Envia para Object Storage
        try:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=file_id,
                data=BytesIO(raw_bytes),
                length=size_bytes,
                content_type=mime_type,
            )
        except S3Error as e:
            logger.error("Erro ao enviar arquivo para o Object Storage: %r", e)
            raise

        self._save_metadata_in_db(
            file_id=file_id,
            uploader=uploader,
            conversation_id=conversation_id,
            filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
            checksum=checksum,
        )

        logger.info(
            "Arquivo (upload simples) salvo: file_id=%s size=%d checksum=%s",
            file_id,
            size_bytes,
            checksum[:16] + "...",
        )
        return file_id

    def _save_metadata_in_db(
        self,
        file_id: str,
        uploader: str,
        conversation_id: str,
        filename: str,
        mime_type: str,
        size_bytes: int,
        checksum: str,
    ):
        """
        Salva os metadados do arquivo na tabela files do Cassandra.

        Schema esperado:
            file_id text PRIMARY KEY,
            uploader text,
            conversation_id text,
            filename text,
            mime_type text,
            size_bytes bigint,
            checksum text,
            created_at timestamp
        """
        created_at = datetime.now(timezone.utc)
        try:
            self.store.session.execute(
                """
                INSERT INTO files (
                    file_id, uploader, conversation_id,
                    filename, mime_type, size_bytes,
                    checksum, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    file_id,
                    uploader,
                    conversation_id,
                    filename,
                    mime_type,
                    size_bytes,
                    checksum,
                    created_at,
                ),
            )
        except Exception as e:
            logger.error("Erro ao salvar metadados de arquivo no Cassandra: %r", e)
            raise

    # Upload multipart/chunked
    def init_multipart_upload(
        self,
        uploader: str,
        conversation_id: str,
        filename: str,
        mime_type: str,
    ) -> str:
        """
        Cria um upload_id e guarda metadados iniciais em um arquivo meta.json.
        """
        upload_id = self._generate_uuid()
        base_path = self._multipart_path(upload_id)
        os.makedirs(base_path, exist_ok=True)

        meta = {
            "uploader": uploader,
            "conversation_id": conversation_id,
            "filename": filename,
            "mime_type": mime_type,
        }
        with open(self._multipart_meta_path(upload_id), "w", encoding="utf-8") as f:
            json.dump(meta, f)

        logger.info(
            "Upload multipart iniciado: upload_id=%s filename=%s conv=%s",
            upload_id,
            filename,
            conversation_id,
        )
        return upload_id

    def save_multipart_chunk(
        self,
        upload_id: str,
        chunk_index: int,
        fobj: BinaryIO,
    ):
        """
        Salva um chunk em disco local:
            uploads_tmp/<upload_id>/chunk_<index>.bin

        Se o chunk já existir, ele é sobrescrito (permite reenvio).
        """
        base_path = self._multipart_path(upload_id)
        if not os.path.isdir(base_path):
            raise ValueError(f"Upload ID inválido ou expirado: {upload_id}")

        chunk_path = os.path.join(base_path, f"chunk_{chunk_index:06d}.bin")
        data = fobj.read()
        if isinstance(data, str):
            data = data.encode("utf-8")

        with open(chunk_path, "wb") as f:
            f.write(data)

        logger.info(
            "Chunk salvo: upload_id=%s chunk_index=%d size=%d",
            upload_id,
            chunk_index,
            len(data),
        )

    def complete_multipart_upload(self, upload_id: str) -> str:
        """
        Junta todos os chunks em ordem, envia para o Object Storage,
        salva metadados no Cassandra e retorna file_id.
        """
        base_path = self._multipart_path(upload_id)
        meta_path = self._multipart_meta_path(upload_id)

        if not os.path.isfile(meta_path):
            raise ValueError(f"Upload ID inválido (sem meta.json): {upload_id}")

        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        chunks = [
            f for f in os.listdir(base_path) if f.startswith("chunk_") and f.endswith(".bin")
        ]
        if not chunks:
            raise ValueError(f"Nenhum chunk encontrado para upload_id={upload_id}")

        chunks.sort()

        from io import BytesIO

        buffer = BytesIO()
        for chunk_name in chunks:
            chunk_path = os.path.join(base_path, chunk_name)
            with open(chunk_path, "rb") as cf:
                buffer.write(cf.read())

        raw_bytes = buffer.getvalue()
        checksum, size_bytes = self._compute_checksum_and_size(raw_bytes)

        file_id = self._generate_uuid()

        try:
            from io import BytesIO as BY

            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=file_id,
                data=BY(raw_bytes),
                length=size_bytes,
                content_type=meta["mime_type"],
            )
        except S3Error as e:
            logger.error("Erro ao enviar arquivo multipart para Object Storage: %r", e)
            raise

        self._save_metadata_in_db(
            file_id=file_id,
            uploader=meta["uploader"],
            conversation_id=meta["conversation_id"],
            filename=meta["filename"],
            mime_type=meta["mime_type"],
            size_bytes=size_bytes,
            checksum=checksum,
        )

        logger.info(
            "Upload multipart concluído: upload_id=%s file_id=%s size=%d checksum=%s",
            upload_id,
            file_id,
            size_bytes,
            checksum[:16] + "...",
        )

        try:
            for fname in os.listdir(base_path):
                os.remove(os.path.join(base_path, fname))
            os.rmdir(base_path)
        except Exception as e:
            logger.warning("Falha ao limpar diretório multipart %s: %r", base_path, e)

        return file_id

    # Download
    def open_file(self, file_id: str) -> Optional[str]:
        """
        Faz download do objeto do MinIO/S3 para um arquivo temporário em disco e
        retorna o caminho (para o FileResponse do FastAPI).
        """
        tmp_path = self._temp_path(file_id)

        try:
            response = self.client.get_object(self.bucket_name, file_id)
            with open(tmp_path, "wb") as f:
                for chunk in response.stream(32 * 1024):
                    f.write(chunk)
            response.close()
            response.release_conn()
        except S3Error as e:
            logger.error("Erro ao baixar arquivo do Object Storage: %r", e)
            return None

        if not os.path.exists(tmp_path):
            logger.warning("Arquivo temporário não encontrado após download: %s", tmp_path)
            return None

        return tmp_path

    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        """
        Recupera os metadados do arquivo no Cassandra.
        """
        try:
            row = self.store.session.execute(
                "SELECT file_id, uploader, conversation_id, filename, mime_type, "
                "size_bytes, checksum, created_at FROM files WHERE file_id = %s",
                (file_id,),
            ).one()
        except Exception as e:
            logger.error("Erro ao buscar metadados de arquivo: %r", e)
            return None

        if row is None:
            return None

        return {
            "file_id": row.file_id,
            "uploader": row.uploader,
            "conversation_id": row.conversation_id,
            "filename": row.filename,
            "mime_type": row.mime_type,
            "size_bytes": row.size_bytes,
            "checksum": row.checksum,
            "created_at": row.created_at.isoformat()
            if row.created_at is not None
            else None,
        }

    def generate_presigned_url(self, file_id: str, expires_sec: int = 600) -> Optional[str]:
        """
        Gera uma URL temporária (presigned) para baixar o arquivo direto do MinIO/S3.
        """
        try:
            url = self.client.get_presigned_url(
                method="GET",
                bucket_name=self.bucket_name,
                object_name=file_id,
                expires=expires_sec,
            )
            return url
        except S3Error as e:
            logger.error("Erro ao gerar presigned URL: %r", e)
            return None
