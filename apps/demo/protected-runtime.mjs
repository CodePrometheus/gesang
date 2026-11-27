import { readFile } from "node:fs/promises";

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const terminal = new Set(["completed", "stopped", "failed"]);

function problem(statusCode, message, cause) {
  return Object.assign(new Error(message, { cause }), { statusCode });
}

function required(env, name) {
  const value = env[name]?.trim();
  if (!value) throw new Error(`Set ${name} to connect to the protected host.`);
  return value;
}

function positive(value, name, max = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!/^[1-9][0-9]*$/.test(value) || !Number.isSafeInteger(number) || number > max) {
    throw new Error(`${name} must be a positive integer in range.`);
  }
  return number;
}

function checkedId(value) {
  if (typeof value !== "string" || !uuid.test(value)) {
    throw problem(400, "A stable identifier is required.");
  }
  return value;
}

function outputText(content) {
  return content.filter((part) => part.type === "text").map((part) => part.text).join("\n");
}

function toolResultText(message, entry) {
  let outcome = message.isError ? "Tool failed" : "Tool completed";
  let content = message.content;
  const diagnostics = entry.data?.diagnostics;
  const result = message.toolName === "command" && Array.isArray(diagnostics)
    ? diagnostics.find((diagnostic) => diagnostic.code === "command_result") : undefined;
  if (result) {
    try {
      const { exit } = JSON.parse(result.message);
      if (exit?.kind === "code" && Number.isInteger(exit.value)) {
        outcome += " (exit code " + exit.value + ")";
        const trailer = content.at(-1);
        const duplicate = "<harness>\n[info] " + result.message + "\n</harness>";
        if (result.severity === "info" && diagnostics.length === 1 &&
            trailer?.type === "text" && trailer.text === duplicate) {
          content = content.slice(0, -1);
        }
      }
    } catch {
      // An older or malformed diagnostic cannot establish an exit status.
    }
  }
  return [outcome, outputText(content)].filter(Boolean).join("\n");
}

function messagesFrom(snapshot) {
  const messages = [];
  for (const entry of snapshot.view.entries) {
    for (const message of entry.model ?? []) {
      const content = message.content;
      if (typeof content === "string" && message.role === "user") {
        messages.push({ role: "user", text: content });
        continue;
      }
      if (!Array.isArray(content)) continue;
      if (message.role === "user" || message.role === "assistant") {
        const text = outputText(content);
        if (text) messages.push({ role: message.role, text });
        for (const part of content) {
          if (part.type === "toolCall") {
            messages.push({ role: "tool", text: `${part.name}\n${JSON.stringify(part.arguments, null, 2)}` });
          }
        }
      } else if (message.role === "toolResult") {
        messages.push({ role: "tool", text: toolResultText(message, entry) });
      }
    }
  }
  return messages;
}

function replyFor(goal) {
  if (terminal.has(goal.status)) return { kind: "continue", version: goal.version };
  if (goal.status === "waiting_user") {
    return { kind: "input", version: goal.version, questionId: goal.waiting?.questionId };
  }
  return undefined;
}

function browserReceipt(receipt) {
  if (!receipt) return null;
  return {
    requestId: receipt.requestId,
    submissionId: receipt.requestId,
    state: receipt.state,
    ...(receipt.engineReceipt && { id: receipt.engineReceipt.goalId }),
  };
}

/** Attach to the owner's existing command and goal gateways. The browser holds no host credential. */
export async function openProtectedRuntime(env) {
  const url = new URL(required(env, "GESANG_DEMO_GOAL_HOST_URL"));
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (!(url.protocol === "https:" || (url.protocol === "http:" && loopback)) ||
      url.username || url.password || url.search || url.hash) {
    throw new Error("The protected host must use HTTPS or loopback HTTP without URL credentials.");
  }
  if (!url.pathname.endsWith("/")) url.pathname += "/";
  const agent = checkedId(required(env, "GESANG_DEMO_AGENT_ID"));
  const grant = {
    id: checkedId(required(env, "GESANG_DEMO_GRANT_ID")),
    version: positive(required(env, "GESANG_DEMO_GRANT_VERSION"), "GESANG_DEMO_GRANT_VERSION"),
  };
  const timeout = positive(required(env, "GESANG_DEMO_HOST_TIMEOUT_MS"), "GESANG_DEMO_HOST_TIMEOUT_MS", 0x7fff_ffff);
  const limit = positive(env.GESANG_DEMO_HISTORY_LIMIT ?? "40", "GESANG_DEMO_HISTORY_LIMIT");
  const credential = (await readFile(required(env, "GESANG_DEMO_OWNER_TOKEN_FILE"), "utf8")).trim();
  if (!credential || /\s/.test(credential)) throw new Error("The owner credential file must contain one token.");

  const agentRoot = `v1/agents/${agent}`;
  const goalPath = (id) => `${agentRoot}/goals/${checkedId(id)}`;
  const delegated = { kind: "grant", ...grant };

  async function call(path, { input, signal, missing = false } = {}) {
    const deadline = AbortSignal.timeout(timeout);
    let response;
    try {
      response = await fetch(new URL(path, url), {
        redirect: "error",
        signal: signal === undefined ? deadline : AbortSignal.any([deadline, signal]),
        headers: {
          Authorization: `Bearer ${credential}`,
          "Accept-Encoding": "identity",
          ...(input && { "Content-Type": "application/json" }),
        },
        ...(input && { method: "POST", body: JSON.stringify(input) }),
      });
    } catch (error) {
      throw problem(503, "Could not reach the protected host.", error);
    }
    if (response.ok) return response;
    await response.body?.cancel();
    if (missing && response.status === 404) return null;
    throw problem(response.status, "The protected host could not complete the request.");
  }

  async function json(path, options) {
    const response = await call(path, options);
    return response?.json();
  }

  function verifyReceipt(receipt, requestId) {
    if (receipt && (receipt.requestId !== requestId || receipt.agentId !== agent)) {
      throw problem(503, "The protected host returned an unrelated receipt.");
    }
    return receipt;
  }

  async function receipt(id) {
    return verifyReceipt(await json(`v1/requests/${checkedId(id)}`, { missing: true }), id);
  }

  return {
    protected: true,
    async listConversations() {
      const page = await json(`${agentRoot}/goals?${new URLSearchParams({ limit })}`);
      return page.goals.map((goal) => ({ id: goal.goalId, title: goal.objective.trim().split(/\r?\n/, 1)[0] }));
    },

    async postMessage(id, text, requestId, reply) {
      checkedId(id);
      checkedId(requestId);
      let idAfterAdmission = id;
      let authorization = delegated;
      let command;
      if (id === requestId && reply === undefined) {
        command = { kind: "goal.submit", objective: text, completionCriteria: [], grant };
      } else {
        if (!reply || !Number.isSafeInteger(reply.version) || reply.version < 1) {
          throw problem(400, "A current conversation reference is required.");
        }
        if (reply.kind === "continue") {
          command = {
            kind: "goal.submit", objective: text, completionCriteria: [], grant,
            continuation: { goalId: id, expectedVersion: reply.version },
          };
          idAfterAdmission = requestId;
        } else if (reply.kind === "input" && (reply.questionId === undefined || typeof reply.questionId === "string")) {
          authorization = { kind: "owner" };
          command = { kind: "goal.input", goalId: id, expectedVersion: reply.version, text, questionId: reply.questionId };
        } else {
          throw problem(400, "The conversation reference is invalid.");
        }
      }
      const admitted = verifyReceipt(
        await json(`${agentRoot}/commands`, { input: { requestId, authorization, command } }),
        requestId,
      );
      return { id: idAfterAdmission, ...browserReceipt(admitted) };
    },

    async getConversation(id, lookup) {
      const path = goalPath(id);
      const firstReceipt = lookup === id ? await receipt(id) : undefined;
      if (firstReceipt?.state === "pending") {
        return { id, status: "running", messages: [], receipt: browserReceipt(firstReceipt) };
      }
      const goal = await json(path, { missing: true });
      const original = firstReceipt === undefined && (lookup !== undefined || !goal)
        ? await receipt(lookup ?? id) : firstReceipt;
      const observedReceipt = lookup === undefined ? {} : { receipt: browserReceipt(original) };
      if (!goal) {
        if (original?.state === "accepted") throw problem(503, "Waiting for the original goal to become available.");
        if (original?.state === "pending" || original?.state === "rejected" || lookup === id) {
          const status = original?.state === "pending" ? "running" : original?.state === "rejected" ? "error" : "idle";
          return { id, status, messages: [], ...observedReceipt };
        }
        return undefined;
      }
      const conversation = await json(`${path}/conversation`);
      const publications = goal.status === "completed" ? await json(`${path}/artifacts`) : null;
      const status = goal.status === "failed" || goal.status === "stopped" ? "error"
        : goal.status === "completed" || goal.status === "waiting_user" ? "idle" : "running";
      return {
        id, status,
        messages: messagesFrom(conversation),
        artifacts: publications?.artifacts ?? [],
        reply: replyFor(goal),
        waiting: goal.status === "waiting_user" ? goal.waiting?.reason : undefined,
        ...observedReceipt,
      };
    },

    readPublishedArtifact(id, artifactId, signal) {
      return call(`${goalPath(id)}/artifacts/${checkedId(artifactId)}`, { signal });
    },
    async close() {},
  };
}
