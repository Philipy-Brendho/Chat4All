import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    chat_users: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "5s", target: 5 },
        { duration: "20s", target: 20 },
        { duration: "10s", target: 0 },
      ],
      gracefulStop: "10s",
    }
  },
};

const baseUrl = "http://localhost:8000";

const users = [
  { username: "alice", password: "123" },
  { username: "bob", password: "123" },
  { username: "ju", password: "123" },
];

export default function () {
  const user = users[Math.floor(Math.random() * users.length)];

  // LOGIN
  const loginRes = http.post(`${baseUrl}/login`, JSON.stringify(user), {
    headers: { "Content-Type": "application/json" },
  });

  check(loginRes, {
    "login status 200": (r) => r.status === 200,
  });

  const token = loginRes.json("token");
  const auth = { headers: { Authorization: `Bearer ${token}` } };

  // LISTAR ONLINE
  http.get(`${baseUrl}/online`, auth);

  const groupName = "grupoSD";

  // CRIAR GRUPO
  http.post(
    `${baseUrl}/groups`,
    JSON.stringify({ name: groupName }),
    { headers: { ...auth.headers, "Content-Type": "application/json" } }
  );

  // ENTRAR NO GRUPO
  http.post(`${baseUrl}/groups/${groupName}/join`, null, auth);

  // ENVIAR MENSAGEM PRIVADA
  const target = "alice";
  http.post(
    `${baseUrl}/v1/messages`,
    {
      to: target,
      text: `Oi ${target} — ${Math.random()}`,
      type: "text",
    },
    auth
  );

  sleep(1);
}
