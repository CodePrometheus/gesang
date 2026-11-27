import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { DAEMON_PATH } from "@chopin-ai/harness-pi";
import { Connection } from "@earendil-works/pi-env";
import { sandboxSocket } from "./sandbox-socket.mjs";

const connector = fileURLToPath(new URL("./sandbox-connect.mjs", import.meta.url));

export function sandboxWorkspaceFromEnv(env) {
  const { GESANG_DEMO_SANDBOX_HOST: host, GESANG_DEMO_SANDBOX_PORT: port, GESANG_DEMO_SANDBOX_TLS_DIR: tlsDir } = env;
  if ([host, port, tlsDir].every((value) => value === undefined)) return undefined;
  if (!host || !tlsDir || !/^\d{1,5}$/.test(port ?? "") || Number(port) < 1 || Number(port) > 65535) {
    throw new Error("Set the sandbox DNS name, TLS port, and TLS credentials directory together.");
  }
  return { host, port: Number(port), tlsDir };
}

/** A sandbox provisioned by the host and reached through its authenticated pi-env relay. */
export function createSandboxWorkspace(target, root) {
  const { host, port, tlsDir } = target;
  const command = [process.execPath, connector, host, String(port), tlsDir];
  const managed = async () => { throw new Error("The host manages the sandbox lifecycle."); };
  async function check() {
    const socket = await sandboxSocket(target);
    try {
      await once(socket, "secureConnect");
    } finally {
      socket.destroy();
    }
  }

  const provider = {
    open(owner) {
      return {
        owner, root,
        async state() { await check(); return "running"; },
        start: check,
        pause: managed,
        stop: managed,
        destroy: managed,
        command(argv, env = {}) {
          if (argv.length !== 1 || argv[0] !== DAEMON_PATH || Object.keys(env).length > 0) {
            throw new Error("The sandbox connector only starts the workspace daemon.");
          }
          return { argv: command, env: {} };
        },
      };
    },
    async list() { return []; },
  };

  // The host reads approved exports through the same sandbox daemon protocol.
  async function readBytes(argv, maxBytes) {
    const connection = new Connection({ command });
    const limit = new AbortController();
    const signal = AbortSignal.any([AbortSignal.timeout(30_000), limit.signal]);
    const chunks = [];
    let size = 0;
    try {
      const { json } = await connection.request("exec", {
        argv,
        cwd: root,
        inheritEnv: false,
        env: { PATH: "/usr/local/bin:/usr/bin:/bin" },
        timeoutMs: 30_000,
      }, {
        signal,
        onEvent(event, bytes) {
          if (event.kind !== "output" || event.stream !== "stdout" || limit.signal.aborted) return;
          size += bytes.length;
          if (size > maxBytes) limit.abort();
          else chunks.push(Buffer.from(bytes));
        },
      });
      if (limit.signal.aborted) throw limit.signal.reason;
      if (!Number.isInteger(json.exitCode)) throw new Error("The sandbox returned an invalid exit status.");
      return { exitCode: json.exitCode, bytes: Buffer.concat(chunks, size) };
    } catch (error) {
      if (limit.signal.aborted) throw new Error("Artifact exceeds the download size limit.", { cause: error });
      throw error;
    } finally {
      connection.close();
    }
  }

  return { provider, readBytes };
}
