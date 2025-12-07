import os
import sys
from typing import List, Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, Path
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse
from pydantic import BaseModel
import grpc
from google.protobuf import empty_pb2
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram
import time

import logging
from pythonjsonlogger import jsonlogger

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

resource = Resource(attributes={
    "service.name": "chat4all-api"
})

provider = TracerProvider(
    resource=resource,
    sampler=TraceIdRatioBased(0.01)
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

try:
    import chat_pb2
    import chat_pb2_grpc
    from auth.jwt_handler import decode_token
    from storage.cassandra_store import CassandraStore
    from storage.file_store import FileStore
except ImportError:
    pass 

GRPC_TARGET = "localhost:50051"

app = FastAPI(
    title="Chat4All v2 – API REST Gateway",
    version="1.0.0",
)

Instrumentator().instrument(app).expose(app, endpoint="/metrics")
# LOGS E TRACING

# logs estruturados em JSON
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
)
handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.handlers = [handler]
root_logger.setLevel(logging.INFO)

# Tracing OTel
resource = Resource(attributes={"service.name": "chat4all-api"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317", insecure=True)
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# instrumenta FastAPI e logging
FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
LoggingInstrumentor().instrument(set_logging_format=False)

file_store = FileStore()
cassandra_store = CassandraStore()

# CONFIGURAÇÃO DE SEGURANÇA
security = HTTPBearer()

def get_token_auth(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Valida se o header 'Authorization: Bearer <token>' existe.
    Retorna a string do token.
    """
    token = credentials.credentials
    if not token:
        raise HTTPException(status_code=401, detail="Token inválido ou ausente")
    return token

# MODELOS Pydantic
class LoginRequestBody(BaseModel):
    username: str
    password: str

class LoginResponseBody(BaseModel):
    ok: bool
    token: str
    message: str

class CreateGroupBody(BaseModel):
    name: str

class SendPrivateMessageBody(BaseModel):
    target: str
    text: str
    channels: Optional[List[str]] = None 

class SendGroupMessageBody(BaseModel):
    group: str
    text: str
    channels: Optional[List[str]] = None 

# Helpers
def get_stub():
    channel = grpc.insecure_channel(GRPC_TARGET)
    return chat_pb2_grpc.ChatStub(channel)

def metadata_from_token(token: str):
    return (("authorization", token),)

def get_username(token: str) -> str:
    user = decode_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Token expirado ou inválido")
    return user

# ENDPOINTS
@app.post("/login", response_model=LoginResponseBody, summary="Login")
def login(body: LoginRequestBody):
    stub = get_stub()
    try:
        resp = stub.Login(
            chat_pb2.LoginRequest(username=body.username, password=body.password)
        )
        return LoginResponseBody(ok=resp.ok, token=resp.token, message=resp.message)
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Erro gRPC: {e.details()}")

@app.get("/groups", summary="Listar grupos")
def list_groups(token: str = Depends(get_token_auth)):
    stub = get_stub()
    try:
        resp = stub.ListGroups(empty_pb2.Empty(), metadata=metadata_from_token(token))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Erro gRPC: {e.details()}")
    return {"groups": list(resp.groups)}

@app.post("/groups", summary="Criar grupo")
def create_group(body: CreateGroupBody, token: str = Depends(get_token_auth)):
    stub = get_stub()
    req = chat_pb2.CreateGroupRequest(group=body.name, creator="ignored")
    try:
        resp = stub.CreateGroup(req, metadata=metadata_from_token(token))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Erro gRPC: {e.details()}")
    
    if not resp.ok:
        raise HTTPException(status_code=400, detail=resp.message)
    return {"ok": resp.ok, "message": resp.message}

@app.post("/groups/{group_name}/join", summary="Entrar em grupo")
def join_group(group_name: str, token: str = Depends(get_token_auth)):
    stub = get_stub()
    req = chat_pb2.JoinGroupRequest(group=group_name, username="ignored")
    try:
        resp = stub.JoinGroup(req, metadata=metadata_from_token(token))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Erro gRPC: {e.details()}")

    if not resp.ok:
        raise HTTPException(status_code=400, detail=resp.message)
    return {"ok": resp.ok, "message": resp.message}

@app.get("/online", summary="Usuários Online")
def list_online_users(token: str = Depends(get_token_auth)):
    stub = get_stub()
    try:
        resp = stub.ListOnlineUsers(empty_pb2.Empty(), metadata=metadata_from_token(token))
        return {"online_users": list(resp.usernames)}
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Erro gRPC: {e.details()}")

# Endpoints V1 Unificados
@app.get("/v1/users/me")
def me(token: str = Depends(get_token_auth)):
    username = get_username(token)
    return {"username": username}

@app.post("/v1/files")
async def upload_file(
    conversation_id: str = Form(...),
    file: UploadFile = File(...),
    token: str = Depends(get_token_auth),
):
    username = get_username(token)
    file_id = file_store.save_file(
        uploader=username,
        conversation_id=conversation_id,
        filename=file.filename,
        mime_type=file.content_type or "application/octet-stream",
        fobj=file.file,
    )
    return {"file_id": file_id}

@app.get("/v1/files/{file_id}")
def download_file(file_id: str, token: str = Depends(get_token_auth)):
    _ = get_username(token)

    meta = file_store.get_file_metadata(file_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Metadados do arquivo não encontrados")

    path = file_store.open_file(file_id)
    if not path:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado no storage")

    return FileResponse(
        path,
        filename=meta["filename"],
        media_type=meta["mime_type"] or "application/octet-stream",
    )

@app.post("/v1/messages")
def send_message(
    to: str = Form(...),
    channel: str = Form("internal"),
    type: str = Form("text"),
    text: str = Form(""),
    file_id: str = Form(""),
    token: str = Depends(get_token_auth),
):
    username = get_username(token)
    with grpc.insecure_channel(GRPC_TARGET) as ch:
        stub = chat_pb2_grpc.ChatStub(ch)
        
        payload = chat_pb2.MessagePayload(
            type=type,
            text=text,
            file_id=file_id
        )

        req = chat_pb2.SendMessageRequest(
            **{"from": username},
            to=[to],
            channels=[channel],
            payload=payload,
        )
        
        ack = stub.SendMessage(req, metadata=metadata_from_token(token))
        return {"ok": ack.ok, "message": ack.message}

@app.get("/v1/conversations/{conversation_id}/messages")
def list_messages(
    conversation_id: str,
    token: str = Depends(get_token_auth)
):
    _ = get_username(token)

    rows = cassandra_store.session.execute("""
        SELECT * FROM messages_by_conversation
        WHERE conversation_id = %s
    """, (conversation_id, ))

    messages = []
    for r in rows:
        messages.append({
            "message_id": r.message_id,
            "sender": r.sender,
            "payload": {
                "type": r.payload_type,
                "text": r.payload_text,
                "file_id": r.payload_file_id,
            },
            "status": r.status,
            "timestamp": r.ts_unix_ms
        })

    return messages

@app.get("/v1/conversations")
def list_conversations(token: str = Depends(get_token_auth)):
    _ = get_username(token)

    rows = cassandra_store.session.execute("""
        SELECT DISTINCT conversation_id
        FROM messages_by_conversation
    """)

    return {"conversations": [r.conversation_id for r in rows]}

# MULTIPART / CHUNKED UPLOAD
class MultipartInitiateBody(BaseModel):
    conversation_id: str
    filename: str
    mime_type: str = "application/octet-stream"

@app.post("/v1/files/multipart/initiate")
def multipart_initiate(
    body: MultipartInitiateBody,
    token: str = Depends(get_token_auth),
):
    """
    Inicia um upload multipart e devolve upload_id.
    """
    username = get_username(token)

    upload_id = file_store.init_multipart_upload(
        uploader=username,
        conversation_id=body.conversation_id,
        filename=body.filename,
        mime_type=body.mime_type,
    )
    return {"upload_id": upload_id}

@app.put("/v1/files/multipart/{upload_id}/chunk")
async def multipart_chunk(
    upload_id: str = Path(...),
    chunk_index: int = Form(..., description="Índice do chunk, começando em 0 ou 1"),
    chunk: UploadFile = File(...),
    token: str = Depends(get_token_auth),
):
    """
    Envia um chunk para o upload_id informado.
    """
    _ = get_username(token)

    try:
        file_store.save_multipart_chunk(
            upload_id=upload_id,
            chunk_index=chunk_index,
            fobj=chunk.file,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"ok": True, "upload_id": upload_id, "chunk_index": chunk_index}

@app.post("/v1/files/multipart/{upload_id}/complete")
def multipart_complete(
    upload_id: str = Path(...),
    token: str = Depends(get_token_auth),
):
    """
    Conclui o upload multipart, junta os chunks, envia para o Object Storage
    e devolve file_id.
    """
    _ = get_username(token)

    try:
        file_id = file_store.complete_multipart_upload(upload_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao completar upload: {e!r}")

    return {"file_id": file_id}

@app.get("/v1/files/{file_id}/presigned")
def get_presigned_url(
    file_id: str = Path(...),
    token: str = Depends(get_token_auth),
):
    _ = get_username(token)

    url = file_store.generate_presigned_url(file_id)
    if not url:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado no Object Storage")

    return {"url": url}

# Contador de mensagens enviadas via API
MESSAGES_TOTAL = Counter(
    "chat4all_messages_total",
    "Numero total de mensagens aceitas pela API",
    ["route"]
)

# Histograma de latência da API de envio de mensagens
MESSAGE_LATENCY = Histogram(
    "chat4all_message_latency_seconds",
    "Latencia para aceitar uma mensagem (segundos)",
    ["route"]
)
