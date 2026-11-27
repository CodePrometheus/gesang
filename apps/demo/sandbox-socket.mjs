import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { connect } from "node:tls";

export async function sandboxSocket({ host, port, tlsDir }) {
  const [ca, cert, key] = await Promise.all(["ca.crt", "tls.crt", "tls.key"].map((file) => readFile(join(tlsDir, file))));
  const socket = connect({ host, port, servername: host, ca, cert, key, rejectUnauthorized: true, minVersion: "TLSv1.2" });
  const timer = setTimeout(() => socket.destroy(new Error("Sandbox TLS connection timed out.")), 10_000);
  const cancelTimeout = () => clearTimeout(timer);
  socket.once("secureConnect", cancelTimeout);
  socket.once("close", cancelTimeout);
  return socket;
}
