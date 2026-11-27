import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { json } from "node:stream/consumers";

export const modelId = "smoke-fixture";

function complete(response, delta, finishReason) {
  const chunk = {
    id: `chatcmpl-${randomUUID()}`,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1_000),
    model: modelId,
  };
  const event = (delta, finish_reason) => `data: ${JSON.stringify({
    ...chunk, choices: [{ index: 0, delta, finish_reason }],
  })}\n\n`;
  response.writeHead(200, { "content-type": "text/event-stream" });
  response.end(event({ role: "assistant", ...delta }, null) + event({}, finishReason) + "data: [DONE]\n\n");
}

function call(response, tools, name, args) {
  assert(tools?.some(({ function: tool }) => tool?.name === name), `Missing tool: ${name}`);
  complete(response, { tool_calls: [{
    index: 0,
    id: `call_smoke_${name}`,
    type: "function",
    function: { name, arguments: JSON.stringify(args) },
  }] }, "tool_calls");
}

/** Start a loopback-only model substitute for the smoke prompt. */
export async function startModelFixture({ beforeWrite } = {}) {
  const server = createServer(async (request, response) => {
    if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
      response.writeHead(404).end();
      return;
    }
    try {
      const { model, stream, messages, tools } = await json(request);
      assert.equal(model, modelId);
      assert.equal(stream, true);
      const user = messages.findLast(({ role }) => role === "user");
      const prompt = typeof user?.content === "string"
        ? user.content
        : user?.content?.filter(({ type }) => type === "text").map(({ text }) => text).join("\n");
      const input = /^Input: ([a-f0-9]{32})$/m.exec(prompt ?? "")?.[1];
      assert(input, "Missing smoke input");
      const script = `smoke-${input}.py`;
      const artifact = `smoke-${input}.txt`;
      const last = messages.at(-1);

      if (last.role === "user") {
        await beforeWrite?.();
        call(response, tools, "write", { path: script, content: [
          "from hashlib import sha256",
          "from pathlib import Path",
          `digest = sha256(${JSON.stringify(input)}.encode('utf-8')).hexdigest()`,
          `path = Path('/workspace/exports/${artifact}')`,
          "path.parent.mkdir(parents=True, exist_ok=True)",
          "path.write_bytes((digest + '\\n').encode('utf-8'))",
          "print(digest)",
          "",
        ].join("\n") });
      } else if (last.role === "tool" && last.tool_call_id === "call_smoke_write") {
        assert(last.content.includes(script), "The write tool did not report the script path");
        call(response, tools, "bash", { command: `python3 ${script} && printf 'PYTHON_EXIT=0\\n'` });
      } else {
        assert.equal(last.role, "tool");
        assert.equal(last.tool_call_id, "call_smoke_bash");
        assert.match(last.content, /^PYTHON_EXIT=0$/m, "The Python process did not report a successful exit");
        complete(response, { content: `Download ${artifact}.` }, "stop");
      }
    } catch (error) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: error.message } }));
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}/v1`,
    close: () => new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    }),
  };
}

if (import.meta.main) {
  const fixture = await startModelFixture();
  console.log(JSON.stringify({ baseUrl: fixture.baseUrl, model: modelId, pid: process.pid }));
}
