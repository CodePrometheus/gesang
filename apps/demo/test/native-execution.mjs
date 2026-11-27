import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { modelCatalog } from "../runtime.mjs";

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
const producer = required("GESANG_TEST_CHOPIN_SOURCE");
const moduleAt = (path) => import(pathToFileURL(join(producer, path)).href);
const [{ ControlHost }, { Vault }, { openCloudRuntime }, { openEncryptedStore, verifyPublishedArtifact }] = await Promise.all([
  moduleAt("apps/tui/dist/control.js"),
  moduleAt("apps/tui/dist/vault.js"),
  moduleAt("apps/tui/dist/cloud.js"),
  moduleAt("packages/harness-pi/dist/index.js"),
]);
const root = required("GESANG_TEST_NATIVE_CHAT_DIR");
const identity = {
  agentId: required("CHOPIN_AGENT_ID"),
  epoch: Number(required("CHOPIN_RUNTIME_EPOCH")),
  owner: required("CHOPIN_ENGINE_OWNER"),
  url: required("CHOPIN_CONTROL_URL"),
};
const controlToken = required("CHOPIN_CONTROL_TOKEN");
const policy = JSON.parse(required("CHOPIN_TEST_COMMAND_POLICY"));
const nativeModule = required("CHOPIN_STORAGE_NATIVE");
const volume = join(root, "volume");
const home = join(root, "home");
await mkdir(volume, { mode: 0o700 });
await mkdir(home, { mode: 0o700 });
const bootstrap = await ControlHost.connect(identity, controlToken);
const key = Buffer.from(required("CHOPIN_TEST_STORE_KEY"), "base64url");
delete process.env.CHOPIN_TEST_STORE_KEY;
try {
  const vault = await Vault.initialize(volume, home, key, async () => {});
  const protectedStore = vault.store;
  try {
    const database = await openEncryptedStore({
      ...protectedStore,
      anchor: bootstrap.anchors(),
      nativeModule,
      queueCapacity: 1,
      create: true,
    });
    await database.close(context);
  } finally {
    protectedStore.key.fill(0);
    await vault.close();
  }
} finally {
  key.fill(0);
  await bootstrap.close();
}
const modelId = required("GESANG_TEST_MODEL_ID");
const baseUrl = required("GESANG_TEST_MODEL_URL");
const maxOutputTokens = Number(process.env.GESANG_TEST_MODEL_MAX_OUTPUT_TOKENS ?? "2048");
assert(Number.isSafeInteger(maxOutputTokens) && maxOutputTokens > 0);
const models = modelCatalog({
  baseUrl,
  modelIds: [modelId],
  apiKey: "loopback",
  maxOutputTokens,
  reasoningOffValue: process.env.GESANG_TEST_MODEL_REASONING_OFF_VALUE,
  maxTokensField: process.env.GESANG_TEST_MODEL_MAX_TOKENS_FIELD,
});
const selection = { provider: models.getProviders()[0].id, modelId };
const expected = createHash("sha256").update("5050\n").digest("hex");
const artifact = { path: "result.txt", sha256: expected };
const runtime = await openCloudRuntime({
  version: 1,
  purpose: "execution",
  keySource: "custody",
  volume, home,
  control: identity,
  storage: { nativeModule, queueCapacity: 1 },
  server: { port: 0 },
  model: selection,
  reviewModel: selection,
  jobs: {
    transport: { url: required("CHOPIN_TEST_JOB_URL"), scopeId: identity.agentId },
    domain: required("CHOPIN_TEST_JOB_DOMAIN"), cwd: "/workspace",
    policyRevision: "fixture", inheritEnv: false, policy,
  },
  goalRetryAfterMs: 250,
  vaultSyncMs: 1000,
}, {
  controlToken,
  jobToken: required("CHOPIN_TEST_JOB_CREDENTIAL"),
  launchToken: required("CHOPIN_TEST_LAUNCH_TOKEN"),
}, {
  models: async () => models,
  report: (error) => console.error("execution:", error),
  review: async ({ entries, goal }, reviewContext) => {
    const { store } = await runtime.service.goals(identity.owner);
    let cursor;
    do {
      const page = await entries(16, cursor);
      for (const entry of page.items) {
        if (entry.byTaskId === undefined || !entry.model?.some((message) =>
          message.role === "toolResult" && message.toolName === "command" && !message.isError
        )) continue;
        try {
          await verifyPublishedArtifact(store, goal.goalId, artifact, Number(entry.byTaskId), reviewContext);
        } catch (error) {
          if (error.message === "the successful tool task has no matching published artifact") continue;
          throw error;
        }
        return {
          kind: "complete",
          criteria: ["A Python command produces the requested result and publishes result.txt."],
          summary: "The original tool result and file were verified.",
          evidence: [{ kind: "tool", criterion: 0, entryId: Number(entry.id), artifact }],
        };
      }
      cursor = page.next;
    } while (cursor !== undefined);
    return { kind: "wait", reason: "The command did not produce the required result yet." };
  },
});
const input = createInterface({ input: process.stdin });
try {
  console.log(JSON.stringify({ state: "restored", port: runtime.server.port }));
  for await (const line of input) {
    assert.equal(line, "stop");
    break;
  }
} finally {
  input.close();
  await runtime.close();
  process.stdin.destroy();
}
