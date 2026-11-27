import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { ContainerWorkspaceProvider, containerCli } from "@chopin-ai/workspace";
import { createSandboxWorkspace } from "./sandbox-workspace.mjs";

const execFileAsync = promisify(execFile);
const docker = containerCli("docker");
const workspaceRoot = "/workspace";
// Match the container adapter's output budget.
const maxArtifactBytes = 16 * 1024 * 1024;
const proxyVariables = ["HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "ALL_PROXY", "NO_PROXY"];

const readExport = `
import base64
import os
import stat
import sys

limit = int(sys.argv[2])
try:
    directory = os.open("/workspace/exports", os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(sys.argv[1], os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    finally:
        os.close(directory)
    with os.fdopen(fd, "rb") as artifact:
        info = os.fstat(artifact.fileno())
        if not stat.S_ISREG(info.st_mode):
            sys.exit(2)
        if info.st_size > limit:
            sys.exit(3)
        data = artifact.read(limit + 1)
    if len(data) > limit:
        sys.exit(3)
    sys.stdout.buffer.write(base64.b64encode(data) if sys.argv[3:] == ["base64"] else data)
except OSError:
    sys.exit(2)
`;

function readLimits() {
  const cpus = process.env.GESANG_DEMO_WORKSPACE_CPUS;
  const memory = process.env.GESANG_DEMO_WORKSPACE_MEMORY;
  const pids = process.env.GESANG_DEMO_WORKSPACE_PIDS;
  if (cpus !== undefined && (!Number.isFinite(Number(cpus)) || Number(cpus) <= 0)) {
    throw new Error("GESANG_DEMO_WORKSPACE_CPUS must be a positive number.");
  }
  if (memory !== undefined && !/^[1-9]\d*[bkmg]?$/i.test(memory)) {
    throw new Error("GESANG_DEMO_WORKSPACE_MEMORY must be a positive Docker memory limit.");
  }
  if (pids !== undefined && (!Number.isSafeInteger(Number(pids)) || Number(pids) <= 0)) {
    throw new Error("GESANG_DEMO_WORKSPACE_PIDS must be a positive integer.");
  }
  return {
    cpus: cpus === undefined ? undefined : Number(cpus),
    memory,
    pids: pids === undefined ? undefined : Number(pids),
  };
}

function requestError(statusCode, message) {
  return Object.assign(new Error(message), { statusCode });
}

function createContainerProvider({ image, name }) {
  if (typeof image !== "string" || image === "" || image.startsWith("-")) {
    throw new Error("A workspace image is required.");
  }
  // The container adapter embeds this prefix in a Docker name filter.
  if (typeof name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(name)) {
    throw new Error("Invalid workspace name.");
  }
  const limits = readLimits();
  return new ContainerWorkspaceProvider({
    image,
    prefix: name,
    mountPath: workspaceRoot,
    limits,
    cli: (args) => {
      if (args[0] === "create") {
        return docker([
          "create", "--pull", "never", "--network", "none", "--cap-drop", "ALL", "--user", "10001:10001",
          // Docker's client can otherwise inject host proxy settings into a new container.
          ...proxyVariables.flatMap((key) => ["--env", `${key}=`, "--env", `${key.toLowerCase()}=`]),
          ...(limits.memory === undefined ? [] : ["--memory-swap", limits.memory]),
          ...args.slice(1),
        ]);
      }
      if (args[0] === "ps" && args[3].startsWith("name=")) {
        return docker([...args, "--filter", `label=chopin.provider=${name}`]);
      }
      return docker(args);
    },
  });
}

function readContainerBytes(provider) {
  return async (argv, maxBytes) => {
    const workspaces = await provider.list();
    if (workspaces.length === 0) return { exitCode: 2 };
    if (workspaces.length !== 1 || await workspaces[0].state() !== "running") {
      throw new Error("The workspace is unavailable.");
    }
    const { argv: [binary, ...args] } = workspaces[0].command(argv);
    try {
      const { stdout } = await execFileAsync(binary, args, {
        encoding: "buffer", maxBuffer: maxBytes, timeout: 30_000,
      });
      return { exitCode: 0, bytes: stdout };
    } catch (error) {
      if (typeof error.code === "number") return { exitCode: error.code };
      throw error;
    }
  };
}

/** Attach the execution service and export reader to the configured workspace. */
export function createWorkspace({ image, name, sandbox }) {
  const containerProvider = sandbox === undefined ? createContainerProvider({ image, name }) : undefined;
  const { provider, readBytes } = containerProvider
    ? { provider: containerProvider, readBytes: readContainerBytes(containerProvider) }
    : createSandboxWorkspace(sandbox, workspaceRoot);

  async function readArtifact(filename) {
    if (typeof filename !== "string" || filename === "" || filename === "." || filename === ".." ||
        /[/\\\x00-\x1f\x7f]/.test(filename)) {
      throw requestError(400, "Invalid artifact name.");
    }
    // The pi-env exec protocol transports text. Encode remote files without losing binary bytes.
    const argv = ["/usr/bin/env", "python3", "-I", "-S", "-c", readExport, filename, String(maxArtifactBytes)];
    const encoded = sandbox !== undefined;
    if (encoded) argv.push("base64");
    const wireLimit = encoded ? 4 * Math.ceil(maxArtifactBytes / 3) : maxArtifactBytes;
    const { exitCode, bytes } = await readBytes(argv, wireLimit);
    if (exitCode === 2) throw requestError(404, "Artifact not found.");
    if (exitCode === 3) throw new Error("Artifact exceeds the download size limit.");
    if (exitCode !== 0) throw new Error("Could not read the artifact.");
    if (!encoded) return bytes;
    const wire = bytes.toString("utf8");
    const file = Buffer.from(wire, "base64");
    if (file.length > maxArtifactBytes || file.toString("base64") !== wire) {
      throw new Error("The sandbox returned invalid artifact bytes.");
    }
    return file;
  }

  return { provider, readArtifact };
}
