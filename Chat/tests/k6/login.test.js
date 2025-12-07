import http from "k6/http";
import { check } from "k6";

export default function () {
  const baseUrl = "http://localhost:8000";

  const loginRes = http.post(
    `${baseUrl}/login`,
    JSON.stringify({ username: "alice", password: "123" }),
    { headers: { "Content-Type": "application/json" } }
  );

  check(loginRes, { "login status 200": (r) => r.status === 200 });

  const token = loginRes.json("token");
  console.log("TOKEN:", token);

  const auth = { headers: { Authorization: `Bearer ${token}` } };

  const online = http.get(`${baseUrl}/online`, auth);
  console.log("ONLINE:", online.status, online.body);

  check(online, { "online status 200": (r) => r.status === 200 });
}
