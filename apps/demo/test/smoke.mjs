import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { once } from "node:events";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { text } from "node:stream/consumers";
import { setTimeout as delay } from "node:timers/promises";
import { parseArgs } from "node:util";

const usage = "Usage: node test/smoke.mjs --base-url <url> --model <id> [--timeout-ms <milliseconds>]";
const exitLine = "PYTHON_EXIT=0";

function submit(url, prompt, signal) {
  const body = JSON.stringify({ text: prompt });
  const send = url.protocol === "https:" ? httpsRequest : httpRequest;
  return new Promise((resolve, reject) => {
    const request = send(url, {
      method: "POST",
      agent: false,
      headers: {
        origin: url.origin,
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
        connection: "close",
      },
      signal,
    }, async (response) => {
      try {
        // Start observing only after the submission socket has actually closed.
        const [reply] = await Promise.all([text(response), once(response.socket, "close")]);
        assert.equal(response.statusCode, 202, `Submit message returned HTTP ${response.statusCode}: ${reply}`);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
    request.once("error", reject);
    request.end(body);
  });
}

async function smoke(base, model, signal) {
  async function api(path, method = "GET", body) {
    const response = await fetch(new URL(path, base), {
      method,
      headers: body && { origin: base.origin, "content-type": "application/json" },
      body: body && JSON.stringify(body),
      redirect: "error",
      signal,
    });
    if (!response.ok) throw new Error(`${method} ${path} returned HTTP ${response.status}: ${await response.text()}`);
    return response;
  }

  const { models } = await (await api("/api/models")).json();
  assert(Array.isArray(models) && models.includes(model), `Model is not configured: ${model}`);
  const { id } = await (await api("/api/conversations", "POST", { model })).json();
  assert(typeof id === "string" && id.length > 0, "Conversation creation returned no ID");
  const conversation = `/api/conversations/${encodeURIComponent(id)}`;

  const input = randomBytes(16).toString("hex");
  const script = `smoke-${input}.py`;
  const artifact = `smoke-${input}.txt`;
  const digest = createHash("sha256").update(input, "utf8").digest("hex");
  const prompt = [
    `Write a short Python script named ${script} in the workspace and run it with Python 3.`,
    `Input: ${input}`,
    "Have the script compute the lowercase SHA-256 hex digest of the UTF-8 input above, with no trailing newline in the input.",
    `Print the digest on its own line and write the digest followed by a single newline to /workspace/exports/${artifact}.`,
    `In the same shell tool call, print ${exitLine} on its own line only after the Python process exits successfully.`,
    `When finished, tell me the name of the file I can download, ${artifact}.`,
  ].join("\n");

  console.log(JSON.stringify({ event: "conversation-created", id, model, script, artifact }));
  await submit(new URL(`${conversation}/messages`, base), prompt, signal);
  console.log(JSON.stringify({ event: "submission-disconnected", id }));

  for (;;) {
    const snapshot = await (await api(conversation)).json();
    assert.equal(snapshot.id, id, "Conversation lookup returned a different ID");
    assert(["idle", "running", "error"].includes(snapshot.status), `Unexpected status: ${snapshot.status}`);
    assert(Array.isArray(snapshot.messages), "Conversation returned no message array");
    assert.notEqual(snapshot.status, "error", `The agent reported an error in conversation ${id}`);

    const messages = snapshot.messages;
    const userIndex = messages.findIndex(({ role, text }) => role === "user" && text === prompt);
    const reply = messages.at(-1);
    if (snapshot.status === "idle" && userIndex >= 0 && reply?.role === "assistant") {
      const output = messages.slice(userIndex + 1, -1).find(({ role, text }) => {
        if (role !== "tool") return false;
        const lines = text.split(/\r?\n/);
        return lines.includes(digest) && lines.includes(exitLine);
      });
      assert(output, "No tool result contains both the computed digest and the successful Python exit");
      assert(reply.text.includes(artifact), "Final assistant reply does not identify the download");
      break;
    }
    await delay(1_000, undefined, { signal });
  }

  const download = await api(`/api/artifacts/${encodeURIComponent(artifact)}`);
  assert.match(download.headers.get("content-disposition") ?? "", /^attachment(?:;|$)/i);
  const bytes = Buffer.from(await download.arrayBuffer());
  assert(bytes.equals(Buffer.from(`${digest}\n`)), "Downloaded bytes differ from the independently computed result");
  console.log(JSON.stringify({ event: "smoke-passed", id, model, script, artifact, digest, bytes: bytes.length }));
}

try {
  const { values } = parseArgs({ options: {
    "base-url": { type: "string" },
    model: { type: "string" },
    "timeout-ms": { type: "string" },
    help: { type: "boolean" },
  } });
  if (values.help) {
    console.log(usage);
  } else {
    assert(values["base-url"] && values.model?.trim(), usage);
    const base = new URL(values["base-url"]);
    assert(["http:", "https:"].includes(base.protocol) && !base.username && !base.password,
      "Use an HTTP or HTTPS base URL without embedded credentials");
    const timeout = values["timeout-ms"] ?? process.env.GESANG_TEST_OPERATION_TIMEOUT_MS;
    let signal;
    if (timeout !== undefined) {
      const milliseconds = Number(timeout);
      assert(Number.isInteger(milliseconds) && milliseconds > 0 && milliseconds <= 2_147_483_647,
        "Set the operation timeout to a positive timer interval in milliseconds");
      signal = AbortSignal.timeout(milliseconds);
    }
    await smoke(base, values.model, signal);
  }
} catch (error) {
  console.error(`Smoke test failed: ${error.message}`);
  process.exitCode = 1;
}
