import http from "k6/http";
import { sleep, check } from "k6";

// =================== CONFIGURAÇÃO DOS CENÁRIOS ===================
export const options = {
  scenarios: {
    // Carga principal: envio de mensagens
    messages_scenario: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 5 },  // sobe pra 5 usuários
        { duration: "20s", target: 10 }, // depois pra 10
        { duration: "20s", target: 0 },  // volta pra 0
      ],
      exec: "sendMessages",
    },

    // Upload multipart de arquivos pequenos (simulação)
    uploads_scenario: {
      executor: "constant-vus",
      vus: 2,
      duration: "30s",
      exec: "uploadFiles",
      startTime: "5s",
    },

    // Criação e entrada em grupos
    groups_scenario: {
      executor: "constant-vus",
      vus: 1,
      duration: "20s",
      exec: "groupsScenario",
      startTime: "10s",
    },
  },
};

// =================== CONFIGURAÇÃO BÁSICA ===================
const BASE_URL = "http://localhost:8000";

// Ajuste para o usuário de teste que você já tem cadastrado
const USERNAME = "user1";
const PASSWORD = "123";

// O setup roda UMA vez antes dos cenários
export function setup() {
  // Login na API REST Gateway
  const loginRes = http.post(
    `${BASE_URL}/login`,
    JSON.stringify({
      username: USERNAME,
      password: PASSWORD,
    }),
    {
      headers: { "Content-Type": "application/json" },
    }
  );

  check(loginRes, {
    "login respondeu 200": (r) => r.status === 200,
  });

  const body = loginRes.json();
  const token = body.token;

  check(body, {
    "token existe": () => token !== undefined && token !== "",
  });

  return { token };
}

// Helper para headers com Authorization
function authHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
  };
}

// =================== Cenário 1: Envio de mensagens ===================
export function sendMessages(data) {
  const token = data.token;

  const formData = {
    to: "user2",                // ajuste para algum usuário que exista
    channel: "internal",
    type: "text",
    text: `Mensagem K6 - ts=${Date.now()}`,
    file_id: "",
  };

  const res = http.post(
    `${BASE_URL}/v1/messages`,
    formData,
    { headers: authHeaders(token) }
  );

  check(res, {
    "mensagem 200": (r) => r.status === 200,
  });

  sleep(1);
}

// =================== Cenário 2: Upload multipart ===================
export function uploadFiles(data) {
  const token = data.token;

  // 1) Inicia upload multipart (JSON)
  const convId = "k6-conv-" + Math.floor(Math.random() * 1000);

  const initRes = http.post(
    `${BASE_URL}/v1/files/multipart/initiate`,
    JSON.stringify({
      conversation_id: convId,
      filename: "k6-test.txt",
      mime_type: "text/plain",
    }),
    {
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
    }
  );

  check(initRes, {
    "multipart initiate 200": (r) => r.status === 200,
  });

  const upload_id = initRes.json("upload_id");

  // 2) Envia um único chunk pequeno (texto em memória)
  const chunkBody = {
    chunk_index: "0",
    // k6 monta multipart se encontrar http.file ou campos mistos
    chunk: http.file("conteudo gerado pelo K6\nlinha 2\n", "k6-test.txt"),
  };

  const chunkRes = http.put(
    `${BASE_URL}/v1/files/multipart/${upload_id}/chunk`,
    chunkBody,
    {
      headers: authHeaders(token),
    }
  );

  check(chunkRes, {
    "chunk 200": (r) => r.status === 200,
  });

  // 3) Completa o upload
  const completeRes = http.post(
    `${BASE_URL}/v1/files/multipart/${upload_id}/complete`,
    null,
    {
      headers: authHeaders(token),
    }
  );

  check(completeRes, {
    "complete 200": (r) => r.status === 200,
  });

  // opcional: pegar o file_id gerado
  const file_id = completeRes.json("file_id");

  sleep(1);
}

// =================== Cenário 3: Grupos ===================
export function groupsScenario(data) {
  const token = data.token;

  // Nome de grupo aleatório
  const groupName = `k6-group-${Math.floor(Math.random() * 10000)}`;

  // 1) Criar grupo
  const createRes = http.post(
    `${BASE_URL}/groups`,
    JSON.stringify({ name: groupName }),
    {
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
    }
  );

  check(createRes, {
    "create group 200 or 400": (r) => r.status === 200 || r.status === 400,
  });

  // 2) Entrar no grupo (se já existir, backend decide)
  const joinRes = http.post(
    `${BASE_URL}/groups/${groupName}/join`,
    null,
    {
      headers: authHeaders(token),
    }
  );

  check(joinRes, {
    "join group 200 or 400": (r) => r.status === 200 || r.status === 400,
  });

  sleep(1);
}
