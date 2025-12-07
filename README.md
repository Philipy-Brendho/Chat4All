# Chat4All – Plataforma de Mensageria Distribuída Multicanal

Chat4All é uma plataforma de mensageria distribuída desenvolvida com foco em
escalabilidade horizontal, tolerância a falhas, multicanalidade e observabilidade.
O sistema integra API REST, gRPC, Kafka, Cassandra, Object Storage (MinIO),
conector real com Telegram e mocks para WhatsApp e Instagram.

O objetivo principal é demonstrar, na prática, uma arquitetura orientada a eventos
capaz de processar mensagens internas e externas em tempo real, garantindo
resiliência, rastreabilidade e integração com múltiplos canais.

---

## **Principais Funcionalidades**

- ✔ API REST para envio/recebimento de mensagens  
- ✔ gRPC para streaming de eventos em tempo real  
- ✔ RouterWorker com fan-out para múltiplos canais  
- ✔ Integração real com Telegram  
- ✔ Mocks para WhatsApp e Instagram  
- ✔ Upload de arquivos grandes (até 2 GB) via multipart  
- ✔ Download por presigned URL (não passa pelo backend)  
- ✔ Persistência distribuída em Cassandra  
- ✔ Object Storage com MinIO  
- ✔ Event streaming com Kafka  
- ✔ Observabilidade com Prometheus, Grafana, OpenTelemetry e Jaeger  
- ✔ Logs estruturados em JSON  
- ✔ Suporte a escala horizontal e failover com Kafka consumer groups  

---

##  **Arquitetura Geral**

Cliente → API Gateway → Kafka (chat-incoming) → RouterWorker → { whatsapp-out, instagram-out, telegram-out }
↓
Cassandra + MinIO
↓
Kafka (chat-events)
↓
CLI / gRPC Stream

---

## **Componentes Principais**

### **API Gateway (FastAPI)**
- Endpoints REST (mensagens, arquivos, histórico)  
- Autenticação com JWT  
- Upload multipart e presigned URLs  
- Exposição de `/metrics` para Prometheus  
- Tracing via OpenTelemetry  

### **RouterWorker**
- Consome `chat-incoming`  
- Persiste mensagens no Cassandra  
- Realiza fan-out para canais externos  
- Consome `chat-status` e atualiza status  
- Emite eventos para `chat-events`  
- Implementa tracing + logs estruturados  

### **Conectores**
- **Telegram (real)** usando python-telegram-bot  
- **WhatsApp Mock** (simula entrega e status)  
- **Instagram Mock** (suporte a cross-channel: Telegram → Instagram)  

### **Persistência**
- **Cassandra** para mensagens, status e arquivos  
- **MinIO** para objetos binários (arquivos grandes)  

### **Observabilidade**
- **Prometheus** para métricas  
- **Grafana** para dashboards  
- **Jaeger** para tracing distribuído  
- **Logs JSON** compatíveis com ELK  

---

## **Executando o Projeto**

# Instalação e Configuração do Projeto

Este projeto utiliza **Python**, com suporte a gRPC, FastAPI, Kafka, Cassandra, MinIO e ferramentas de observabilidade como Prometheus e OpenTelemetry.

---

## Pré-requisitos

Antes de começar, verifique se você tem instalado em sua máquina:

- Python **3.9 ou superior**
- pip atualizado

Atualize o pip com:

```bash
python -m pip install --upgrade pip
````

### 1. Clone o repositório e instale as dependências:

```bash
git clone https://github.com/Philipy-Brendho/Chat4All.git
cd Chat4All
````
Instalando dependências(certifique de criar e ativar sua VENV):
```bash
pip install -r Chat/api/requirements.txt
````

### 2. Execute com Docker Compose

```bash
docker-compose up -d
```

Os serviços iniciados incluem:

* Kafka + Zookeeper
* Cassandra
* MinIO
* API Gateway
* RouterWorker
* Conectores
* Prometheus
* Grafana
* Jaeger

---

## **Configuração de Ambiente**

Crie o arquivo `.env` com:

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
CASSANDRA_HOST=cassandra
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
TELEGRAM_BOT_TOKEN=SEU_TOKEN_AQUI
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317

---

## **Fluxo Interno**

Cliente (CLI) → gRPC → API → Kafka (chat-incoming) → RouterWorker → Cassandra → chat-events → CLI

---

## **Fluxo Externo (Cross-Channel)**

Usuário no Telegram → Connector → Kafka → RouterWorker → instagram-out → Instagram Mock → chat-status → RouterWorker → chat-events

---

## **Upload de Arquivos**

* Enviar: `POST /v1/files` (multipart)
* Salva direto no MinIO via streaming
* Metadados no Cassandra
* Download via presigned URL: `GET /v1/files/{id}/presigned`

---

## **Testes de Carga**

Ferramenta: **k6**

Certifique de ter **Chocolatey** instalado em sua máquina e execute o comando para instalação do K6: 
```bash
choco install k6
````

Exemplo de execução:

```bash
export BASE_URL=http://localhost:8000
export TOKEN="seu_token"
k6 run load_test_messages.js
```
---

## **Observabilidade**

### Prometheus

http://localhost:9090

### Grafana

http://localhost:3000
user: admin
pass: admin

### Jaeger

http://localhost:16686

## **Estrutura de Pastas**

/api                 → FastAPI Gateway
/router              → RouterWorker
/proto               → Definição gRPC
/connectors          → Telegram + Mocks
/storage             → CassandraStore + FileStore
/monitoring          → Prometheus + Grafana configs

---

## **Testando o conector Telegram**

No Telegram:

1. Abra o bot
2. Envie uma mensagem
3. Ela aparecerá no RouterWorker
4. Será roteada para Instagram Mock
5. Será retornado status em `chat-status`
6. CLI / API verão atualização em tempo real


---

