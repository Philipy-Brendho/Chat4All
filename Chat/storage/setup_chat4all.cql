CREATE KEYSPACE chat4all
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE chat4all;

-- Mensagens por conversa (ótimo para histórico)
CREATE TABLE messages_by_conversation (
  conversation_id text,
  ts_unix_ms bigint,
  message_id text,
  sender text,
  recipients list<text>,
  channels list<text>,
  payload_type text,
  payload_text text,
  payload_file_id text,
  status text,
  PRIMARY KEY (conversation_id, ts_unix_ms, message_id)
) WITH CLUSTERING ORDER BY (ts_unix_ms ASC, message_id ASC);

-- Para achar rapidamente o remetente de uma mensagem
CREATE TABLE messages_by_id (
  message_id text PRIMARY KEY,
  conversation_id text,
  sender text,
  status text
);

-- Usuários
CREATE TABLE users (
  username text PRIMARY KEY,
  password_hash text
);

CREATE TABLE IF NOT EXISTS users (
    username      text PRIMARY KEY,
    password_hash text
);

-- Grupos
CREATE TABLE groups (
  name text PRIMARY KEY
);

CREATE TABLE group_members (
  group_name text,
  username text,
  PRIMARY KEY (group_name, username)
);

-- Arquivos
CREATE TABLE IF NOT EXISTS files (
    file_id        text PRIMARY KEY,
    uploader       text,
    conversation_id text,
    filename       text,
    mime_type      text,
    size_bytes     bigint,
    checksum       text,
    created_at     timestamp
);

INSERT INTO files (
    file_id, uploader, conversation_id,
    filename, mime_type, size_bytes,
    checksum, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)