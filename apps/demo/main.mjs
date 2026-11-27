import { createHash } from "node:crypto";
import { mkdir, readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { sandboxWorkspaceFromEnv } from "./sandbox-workspace.mjs";
import { openProtectedRuntime } from "./protected-runtime.mjs";
import { openRuntime } from "./runtime.mjs";
import { startServer } from "./server.mjs";

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`Set ${name} before starting Gesang.`);
  return value;
}

async function readModelKey() {
  const keyFile = process.env.GESANG_DEMO_MODEL_API_KEY_FILE;
  if (!keyFile) return required("GESANG_DEMO_MODEL_API_KEY");

  let key;
  try {
    key = (await readFile(keyFile, "utf8")).trim();
  } catch (error) {
    throw new Error("Could not read the model API key file.", { cause: error });
  }
  if (!key) throw new Error("The model API key file is empty.");
  return key;
}

function readPort() {
  const value = process.env.GESANG_DEMO_PORT ?? "8787";
  if (!/^\d{1,5}$/.test(value) || Number(value) > 65535) {
    throw new Error("GESANG_DEMO_PORT must be a port number from 0 to 65535.");
  }
  return Number(value);
}

function readOutputLimit() {
  const value = process.env.GESANG_DEMO_MAX_OUTPUT_TOKENS;
  if (value === undefined) return undefined;
  const count = Number(value);
  if (!/^[1-9][0-9]*$/.test(value) || !Number.isSafeInteger(count)) {
    throw new Error("GESANG_DEMO_MAX_OUTPUT_TOKENS must be a positive integer.");
  }
  return count;
}

async function openDirectRuntime(modelIds) {
  const baseUrl = new URL(required("GESANG_DEMO_MODEL_URL"));
  if (!["http:", "https:"].includes(baseUrl.protocol) || baseUrl.username ||
      baseUrl.password || baseUrl.search || baseUrl.hash) {
    throw new Error("GESANG_DEMO_MODEL_URL must be an HTTP API base URL without credentials, a query or a fragment.");
  }
  const apiKey = await readModelKey();

  const configuredDir = process.env.GESANG_DEMO_DATA_DIR ?? join(homedir(), ".local", "state", "gesang-demo");
  await mkdir(configuredDir, { recursive: true, mode: 0o700 });
  const dataDir = await realpath(configuredDir);
  const workspaceName = `gesang-demo-${createHash("sha256").update(dataDir).digest("hex").slice(0, 20)}`;
  const workspaceImage = process.env.GESANG_DEMO_WORKSPACE_IMAGE ?? "gesang-demo-workspace:local";
  return openRuntime({
    dataDir,
    workspaceName,
    workspaceImage,
    sandbox: sandboxWorkspaceFromEnv(process.env),
    baseUrl: baseUrl.href.replace(/\/$/, ""),
    modelIds,
    apiKey,
    maxOutputTokens: readOutputLimit(),
    maxTokensField: process.env.GESANG_DEMO_MAX_TOKENS_FIELD || undefined,
    reasoningOffValue: process.env.GESANG_DEMO_REASONING_OFF_VALUE || undefined,
  });
}

async function main() {
  const port = readPort();
  const modelIds = [...new Set(required("GESANG_DEMO_MODELS").split(",").map((id) => id.trim()))];
  if (modelIds.some((id) => !id)) throw new Error("GESANG_DEMO_MODELS must list comma-separated model IDs.");
  const protectedHost = process.env.GESANG_DEMO_GOAL_HOST_URL !== undefined;
  if (protectedHost && modelIds.length !== 1) {
    throw new Error("Set GESANG_DEMO_MODELS to the protected host's configured execution model.");
  }
  const runtime = protectedHost ? await openProtectedRuntime(process.env) : await openDirectRuntime(modelIds);

  let server;
  try {
    server = await startServer({ runtime, modelIds, port });
  } catch (error) {
    await runtime.close();
    throw error;
  }
  console.log(`Gesang is listening at ${server.url}`);

  const stop = async () => {
    try {
      await server.close();
    } finally {
      await runtime.close();
    }
  };
  let stopping = false;
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    void stop().catch((error) => {
      console.error("Gesang could not shut down cleanly:", error.message);
      process.exitCode = 1;
    }).finally(() => {
      process.off("SIGINT", shutdown);
      process.off("SIGTERM", shutdown);
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

try {
  await main();
} catch (error) {
  console.error("Gesang could not start:", error.message);
  process.exitCode = 1;
}
