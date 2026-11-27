import { createHash, randomUUID } from "node:crypto";
import { AgentService, inputText, toolText } from "@chopin-ai/harness-pi";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, createProvider } from "@earendil-works/pi-ai";
import { defineDocFamily } from "@earendil-works/pi-durable";
import { stream, streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import { createWorkspace } from "./workspace.mjs";

const OWNER = "demo";
const PROVIDER = "demo";
const MessageReceipt = defineDocFamily({
  kind: "gesang.message-receipt",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  family: true,
  initial: () => ({ digest: null, submissionId: null }),
});

function messageFrom(entry) {
  const update = entry.notification?.update;
  switch (update?.sessionUpdate) {
    case "user_message_chunk":
    case "agent_message_chunk":
      if (update.content.type !== "text") return [];
      return [{
        role: update.sessionUpdate === "user_message_chunk" ? "user" : "assistant",
        text: update.content.text,
      }];
    case "tool_call":
      return [{ role: "tool", text: [update.title, inputText(update.rawInput)].filter(Boolean).join("\n") }];
    case "tool_call_update": {
      const output = toolText(update.content ?? []);
      const status = update.status === "failed" ? "Tool failed" : "Tool completed";
      return [{ role: "tool", text: [status, output].filter(Boolean).join("\n") }];
    }
    default:
      return [];
  }
}

function requestError(statusCode, message) {
  return Object.assign(new Error(message), { statusCode });
}

export function modelCatalog({ baseUrl, modelIds, apiKey, reasoningOffValue, maxOutputTokens, maxTokensField }) {
  if (maxTokensField !== undefined && !["max_tokens", "max_completion_tokens"].includes(maxTokensField)) {
    throw new Error("The model API token limit field must be max_tokens or max_completion_tokens.");
  }
  const models = createModels();
  models.setProvider(createProvider({
    id: PROVIDER,
    baseUrl,
    auth: {
      apiKey: {
        name: "Model API key",
        resolve: async () => ({ auth: { apiKey }, source: "server configuration" }),
      },
    },
    api: { stream, streamSimple },
    models: modelIds.map((id) => ({
      id,
      name: id,
      provider: PROVIDER,
      api: "openai-completions",
      baseUrl,
      input: ["text"],
      reasoning: reasoningOffValue !== undefined,
      ...(reasoningOffValue === undefined ? {} : { thinkingLevelMap: { off: reasoningOffValue } }),
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      // Match the model library's defaults for custom chat models.
      contextWindow: 128_000,
      maxTokens: maxOutputTokens ?? 16_384,
      ...(maxTokensField && { compat: { maxTokensField } }),
    })),
  }));
  return models;
}

/** Opens the single operator's conversation store and workspace-backed execution service. */
export async function openRuntime({ dataDir, workspaceImage, workspaceName, sandbox, ...modelOptions }) {
  const models = modelCatalog(modelOptions);
  const workspace = createWorkspace({ image: workspaceImage, name: workspaceName, sandbox });
  const service = new AgentService({
    dataDir,
    workspaces: workspace.provider,
    models: () => models,
    allowClientMcp: false,
    extensions: [{
      name: "gesang-workspace-order",
      wraps: ["write", "edit", "bash"].map((tool) => ({
        tool,
        wrap: (registered) => ({ ...registered, executionMode: "sequential" }),
      })),
    }],
    layer: {
      instructions: "Wait for a tool result before making a call that depends on it. Write files intended for the user to /workspace/exports. Create the directory if needed and tell the user the file names.",
      skills: [],
      mcpServers: {},
      gates: [],
    },
  });
  const harness = await service.harness(OWNER);

  async function findConversation(id) {
    const number = Number(id);
    if (!Number.isSafeInteger(number) || number < 1 || String(number) !== String(id)) return undefined;
    if (!(await service.isTopLevel(OWNER, number))) return undefined;
    return harness.conversation(number, BACKGROUND_CONTEXT);
  }

  return {
    async listConversations() {
      const summaries = await service.summaries(OWNER, BACKGROUND_CONTEXT);
      return summaries.map(({ id, name, first }) => ({ id: String(id), title: name || first || "New conversation" }));
    },

    async createConversation({ model }) {
      if (!models.getModel(PROVIDER, model)) throw requestError(400, "Model is not configured.");
      const conversation = await harness.createConversation({
        ownership: { kind: "ownerless" },
        agent: { model: { provider: PROVIDER, modelId: model }, thinkingLevel: "off" },
      }, BACKGROUND_CONTEXT);
      return { id: String(conversation.id) };
    },

    async postMessage(id, text, requestId) {
      const conversation = await findConversation(id);
      if (!conversation) throw requestError(404, "Conversation not found.");
      if (requestId === undefined) {
        await conversation.submit({ type: "input", content: text, requestId: randomUUID() }, BACKGROUND_CONTEXT);
        return {};
      }

      const digest = createHash("sha256").update(JSON.stringify(text)).digest("hex");
      return conversation.commit(async (tx) => {
        const receipt = await tx.doc(MessageReceipt, conversation.id, requestId, null);
        if (receipt.digest !== null && receipt.digest !== digest) {
          throw requestError(409, "The request ID already belongs to a different message.");
        }
        if (receipt.submissionId === null) {
          const draft = { type: "input", content: text, requestId: `gesang-message:${requestId}` };
          receipt.submissionId = String(await conversation.submit(tx, draft));
          receipt.digest = digest;
        }
        return { requestId, submissionId: receipt.submissionId };
      }, BACKGROUND_CONTEXT);
    },

    async getConversation(id, requestId) {
      const conversation = await findConversation(id);
      if (!conversation) return undefined;
      // Read submission outcomes before the transcript so an idle snapshot includes the settled answer.
      const submissions = await service.submissions(OWNER, conversation.id, 1);
      const latest = submissions[0];
      let status = "idle";
      if (submissions.some(({ status }) => status === "queued" || status === "placed")) {
        status = "running";
      } else if (latest?.status === "unanswered" && latest.reason !== "aborted") {
        status = "error";
      }
      const transcript = await service.transcript(OWNER, conversation, BACKGROUND_CONTEXT);
      const snapshot = { id: String(conversation.id), status, messages: transcript.flatMap(messageFrom) };
      if (requestId === undefined) return snapshot;
      const receipt = await harness.snapshot(MessageReceipt, conversation.id, requestId, BACKGROUND_CONTEXT);
      return {
        ...snapshot,
        receipt: receipt?.submissionId != null ? { requestId, submissionId: receipt.submissionId } : null,
      };
    },

    readArtifact: (name) => workspace.readArtifact(name),
    close: () => service.close(BACKGROUND_CONTEXT),
  };
}
