import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { json } from "node:stream/consumers";
import { test } from "node:test";
import { openProtectedRuntime } from "../protected-runtime.mjs";
import { startServer } from "../server.mjs";

test("protected chat retains the original request and downloads its published bytes", async (t) => {
  const agent = randomUUID();
  const id = randomUUID();
  const grant = randomUUID();
  const artifactId = randomUUID();
  const bytes = Buffer.from([0, 10, 128, 255]);
  const digest = `sha-256=:${createHash("sha256").update(bytes).digest("base64")}:`;
  const token = "synthetic-owner-token";
  const requests = [];
  const requestPath = `/test/v1/requests/${id}`;
  const agentPath = `/test/v1/agents/${agent}`;
  const goalPath = `${agentPath}/goals/${id}`;
  const goal = { goalId: id, objective: "Produce the example file", status: "completed", version: 3 };
  const publication = { artifactId, name: "receipt.bin" };
  const exit = JSON.stringify({ exit: { kind: "code", value: 0 } });
  let receipt;
  let envelope;
  const host = createServer(async (request, response) => {
    const path = new URL(request.url, "http://localhost").pathname;
    requests.push(path);
    if (request.headers.authorization !== `Bearer ${token}`) {
      return response.writeHead(401).end();
    }
    if (path === `${agentPath}/commands`) {
      envelope = await json(request);
      if (envelope.requestId !== id) return response.writeHead(400).end();
      receipt ??= { requestId: id, agentId: agent, state: "pending", engineReceipt: null };
      return response.writeHead(receipt.state === "pending" ? 202 : 200).end(JSON.stringify(receipt));
    }
    const data = path === requestPath ? receipt
      : path === `${agentPath}/goals` ? { goals: receipt?.engineReceipt ? [goal] : [] }
      : path === `${goalPath}/conversation` ? { view: { entries: [
        { model: [{ role: "user", content: goal.objective }] },
        {
          model: [{ role: "toolResult", toolName: "command", content: [
            { type: "text", text: "done" },
            { type: "text", text: "<harness>\n[info] " + exit + "\n</harness>" },
          ] }],
          data: { diagnostics: [{ code: "command_result", severity: "info", message: exit }] },
        },
      ] } }
      : path === `${goalPath}/artifacts` ? { artifacts: [publication] }
      : path === goalPath && receipt?.engineReceipt ? goal : null;
    if (path === `${goalPath}/artifacts/${artifactId}`) {
      return response.writeHead(200, {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": 'attachment; filename="receipt.bin"',
        "Content-Digest": digest,
      }).end(bytes);
    }
    if (data) return response.writeHead(200).end(JSON.stringify(data));
    response.writeHead(404).end();
  });
  host.listen(0, "127.0.0.1");
  await once(host, "listening");
  t.after(() => new Promise((resolve) => host.close(resolve)));
  const directory = await mkdtemp(join(tmpdir(), "gesang-protected-test-"));
  const tokenFile = join(directory, "token");
  await writeFile(tokenFile, token, { mode: 0o600 });
  const runtime = await openProtectedRuntime({
    GESANG_DEMO_GOAL_HOST_URL: `http://127.0.0.1:${host.address().port}/test`,
    GESANG_DEMO_AGENT_ID: agent,
    GESANG_DEMO_GRANT_ID: grant,
    GESANG_DEMO_GRANT_VERSION: "1",
    GESANG_DEMO_HOST_TIMEOUT_MS: "5000",
    GESANG_DEMO_OWNER_TOKEN_FILE: tokenFile,
  });
  const app = await startServer({ runtime, modelIds: ["Example model"], port: 0 });
  t.after(() => app.close());
  const call = (path, init) => fetch(new URL(path, app.url), init);
  const conversation = `/api/conversations/${id}`;

  const models = await (await call("/api/models")).json();
  assert.deepEqual(models, { models: ["Example model"], protected: true });
  const submitted = await call(`${conversation}/messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: app.url },
    body: JSON.stringify({ text: goal.objective, requestId: id }),
  });
  assert.equal(submitted.status, 202);
  assert.deepEqual(await submitted.json(), { id, requestId: id, submissionId: id, state: "pending" });
  assert.deepEqual(envelope, {
    requestId: id,
    authorization: { kind: "grant", id: grant, version: 1 },
    command: {
      kind: "goal.submit", objective: goal.objective, completionCriteria: [], grant: { id: grant, version: 1 },
    },
  });
  const beforePendingRead = requests.length;
  const pending = await (await call(`${conversation}?receipt=${id}`)).json();
  assert.equal(pending.status, "running");
  assert.equal(pending.receipt.state, "pending");
  assert.deepEqual(requests.slice(beforePendingRead), [requestPath]);

  receipt = { ...receipt, state: "accepted", engineReceipt: { goalId: id } };
  const snapshot = await (await call(`${conversation}?receipt=${id}`)).json();
  assert.equal(snapshot.receipt.id, id);
  assert.equal(snapshot.reply.version, goal.version);
  assert.deepEqual(snapshot.messages, [
    { role: "user", text: goal.objective },
    { role: "tool", text: "Tool completed (exit code 0)\ndone" },
  ]);
  const path = `${conversation}/artifacts/${artifactId}`;
  const before = requests.length;
  const foreign = await call(path, { headers: { "Sec-Fetch-Site": "cross-site" } });
  assert.equal(foreign.status, 403);
  assert.equal(requests.length, before);
  const download = await call(path);
  assert.equal(download.status, 200);
  assert.equal(download.headers.get("Content-Digest"), digest);
  assert.deepEqual(Buffer.from(await download.arrayBuffer()), bytes);
  assert.equal((await call(`/api/conversations/${randomUUID()}/artifacts/${artifactId}`)).status, 404);
  assert.equal((await call("/api/artifacts/receipt.bin")).status, 404);

  receipt = { ...receipt, state: "rejected", engineReceipt: null };
  const repeated = await call(`${conversation}/messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: app.url },
    body: JSON.stringify({ text: goal.objective, requestId: id }),
  });
  assert.equal(repeated.status, 200);
  assert.equal((await repeated.json()).state, "rejected");
});
