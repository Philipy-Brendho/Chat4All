import threading
import grpc
import uuid
import os

import requests
from google.protobuf import empty_pb2

import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import chat_pb2
import chat_pb2_grpc

SERVER_ADDR = "localhost:50051"
API_BASE_URL = "http://localhost:8000"

def reader_thread(stub, username: str, metadata):
    """
    Fica ouvindo o stream de mensagens do servidor e imprime na tela.
    """
    try:
        req = chat_pb2.StreamRequest(username=username)
        for ev in stub.StreamMessages(req, metadata=metadata):
            # Evento de mensagem privada
            if ev.HasField("private_msg"):
                pm = ev.private_msg
                if pm.target == username:
                    print(f"\n[PRIVADO] {pm.sender} → você: {pm.content}")
                else:
                    print(f"\n[PRIVADO] {pm.sender} → {pm.target}: {pm.content}")

            # Evento de mensagem de grupo
            elif ev.HasField("group_msg"):
                gm = ev.group_msg
                print(f"\n[GRUPO {gm.group}] {gm.sender}: {gm.content}")

            # Evento de sistema
            elif ev.HasField("system"):
                print(f"\n[SISTEMA] {ev.system.content}")

            print("> ", end="", flush=True)

    except grpc.RpcError as e:
        print(f"\n[stream encerrado] {e}")

def upload_file_via_api(token: str, conversation_id: str, file_path: str) -> str:
    """
    Faz upload do arquivo para a API REST e retorna o file_id.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    url = f"{API_BASE_URL}/v1/files"
    headers = {"Authorization": f"Bearer {token}"}

    data = {"conversation_id": conversation_id}

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        resp = requests.post(
            url,
            headers=headers,
            data=data,
            files=files,
            timeout=30,
        )

    resp.raise_for_status()
    data = resp.json()
    file_id = data.get("file_id")
    if not file_id:
        raise RuntimeError(f"Resposta da API sem file_id: {data}")

    return file_id

def main():
    with grpc.insecure_channel(SERVER_ADDR) as channel:
        stub = chat_pb2_grpc.ChatStub(channel)

        #LOGIN 
        username = input("Usuário: ").strip()
        password = input("Senha: ").strip()

        login_req = chat_pb2.LoginRequest(username=username, password=password)
        resp = stub.Login(login_req)

        if not resp.ok:
            print("Login falhou:", resp.message)
            return

        token = resp.token
        metadata = (("authorization", token),)

        print(f"Login OK. Token: {token}")
        print("Comandos:")
        print("/p alvo mensagem  → mensagem privada")
        print("/g grupo mensagem → mensagem para grupo")
        print("/cg nome          → cria grupo")
        print("/jg nome          → entra em grupo")
        print("/lg               → lista grupos")
        print("/lo               → lista online")
        print("/file alvo caminho texto  → envia arquivo")
        print("/exit             → sair")

        # THREAD DO STREAM 
        t = threading.Thread(
            target=reader_thread,
            args=(stub, username, metadata),
            daemon=True,
        )
        t.start()

        #LOOP DE COMANDOS 
        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not line:
                continue

            if line == "/exit":
                break

            # /p alvo mensagem
            if line.startswith("/p "):
                try:
                    _, target, msg = line.split(" ", 2)
                except ValueError:
                    print("Uso: /p alvo mensagem")
                    continue

                req = chat_pb2.SendMessageRequest(
                    **{"from": username},
                    to=[target],
                    channels=["internal"],
                    payload=chat_pb2.MessagePayload(type="text", text=msg),
                )
                try:
                    ack = stub.SendMessage(req, metadata=metadata)
                    print(f"[ACK] {ack.message}")
                except grpc.RpcError as e:
                    print(f"[ERRO] {e.code().name}: {e.details()}")
                continue

            # /g grupo mensagem
            if line.startswith("/g "):
                try:
                    _, group, msg = line.split(" ", 2)
                except ValueError:
                    print("Uso: /g grupo mensagem")
                    continue

                req = chat_pb2.SendMessageRequest(
                    **{"from": username},
                    to=[group],
                    channels=["internal"],
                    payload=chat_pb2.MessagePayload(type="text", text=msg),
                )
                try:
                    ack = stub.SendMessage(req, metadata=metadata)
                    print(f"[ACK] {ack.message}")
                except grpc.RpcError as e:
                    print(f"[ERRO] {e.code().name}: {e.details()}")
                continue

            # /cg nome
            if line.startswith("/cg "):
                try:
                    _, group = line.split(" ", 1)
                except ValueError:
                    print("Uso: /cg nome")
                    continue

                req = chat_pb2.CreateGroupRequest(group=group, creator=username)
                ack = stub.CreateGroup(req, metadata=metadata)
                print(f"[CG] {ack.message}")
                continue

            # /jg nome
            if line.startswith("/jg "):
                try:
                    _, group = line.split(" ", 1)
                except ValueError:
                    print("Uso: /jg nome")
                    continue

                req = chat_pb2.JoinGroupRequest(group=group, username=username)
                ack = stub.JoinGroup(req, metadata=metadata)
                print(f"[JG] {ack.message}")
                continue

            # /lg
            if line == "/lg":
                resp_lg = stub.ListGroups(empty_pb2.Empty(), metadata=metadata)
                grupos = list(resp_lg.groups)
                if grupos:
                    print("Grupos:", ", ".join(grupos))
                else:
                    print("Nenhum grupo cadastrado.")
                continue

            # /lo
            if line == "/lo":
                try:
                    resp_lo = stub.ListOnlineUsers(empty_pb2.Empty(), metadata=metadata)
                    online = list(resp_lo.usernames)
                    if online:
                        print("Online:", ", ".join(online))
                    else:
                        print("Nenhum usuário online.")
                except AttributeError:
                    print("Função de listar online não existe nesse proto.")
                continue

            # /file
            if line.startswith("/file "):
                try:
                    _, target, path, msg = line.split(" ", 3)
                except ValueError:
                    print("Uso: /file alvo caminho_do_arquivo texto_opcional")
                    continue

                try:
                    conversation_id = str(uuid.uuid4())

                    print(f"[FILE] Enviando arquivo {path} para API...")
                    file_id = upload_file_via_api(token, conversation_id, path)
                    print(f"[FILE] Upload OK. file_id={file_id}")

                    full_text = f"{msg} [file_id={file_id}]"

                    req = chat_pb2.SendMessageRequest(
                        **{"from": username},
                        to=[target],
                        channels=["internal"],
                        payload=chat_pb2.MessagePayload(
                            type="file",
                            text=full_text,
                            file_id=file_id,
                        ),
                        conversation_id=conversation_id,
                    )
                    ack = stub.SendMessage(req, metadata=metadata)
                    print(f"[ACK] {ack.message}")

                except FileNotFoundError as e:
                    print(f"[FILE ERRO] {e}")
                except Exception as e:
                    print(f"[FILE ERRO] Falha ao enviar arquivo: {e!r}")

                continue

            print("Comando não reconhecido.")

        print("Saindo...")

if __name__ == "__main__":
    main()
