import { sandboxSocket } from "./sandbox-socket.mjs";

const [host, port, tlsDir, command, flag, token, ...extra] = process.argv.slice(2);
if (command !== "serve" || flag !== "--token" || !/^[a-f0-9]{32}$/.test(token ?? "") || extra.length > 0) {
  process.exit(2);
}

const socket = await sandboxSocket({ host, port: Number(port), tlsDir });
socket.once("secureConnect", () => {
  socket.write(`${token}\n`);
  process.stdin.pipe(socket);
  socket.pipe(process.stdout);
});
socket.once("error", (error) => process.stderr.write(`sandbox: ${error.message}\n`));
socket.once("close", (failed) => {
  process.stdin.unpipe(socket);
  process.stdin.pause();
  process.exitCode = failed ? 1 : 0;
});
