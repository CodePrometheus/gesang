import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { setTimeout } from "node:timers/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = process.argv[2];
const chrome = new URL(process.env.GESANG_TEST_NATIVE_CHAT_CHROME_URL);
if (chrome.protocol !== "http:" || chrome.hostname !== "127.0.0.1" ||
    chrome.username || chrome.password || chrome.pathname !== "/" || chrome.hash || chrome.search) {
  throw new Error("Use the loopback HTTP endpoint of a dedicated headless Chrome instance.");
}
const operationTimeout = Number(process.env.GESANG_TEST_OPERATION_TIMEOUT_MS);
const host = await poll(async () => {
  try { return JSON.parse(await readFile(join(root, "host.json"), "utf8")); }
  catch { return null; }
}, "owner gateways");
const bearer = await readFile(host.tokenFile, "utf8");

async function poll(action, label, milliseconds = 20000, interval = 80) {
  let seen;
  const until = performance.now() + milliseconds;
  do {
    seen = await action();
    if (seen) return seen;
    await setTimeout(interval);
  } while (performance.now() < until);
  throw new Error("Timed out: " + label);
}

async function page() {
  const target = await (await fetch(new URL("/json/new?about:blank", chrome), { method: "PUT" })).json();
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve, { once: true });
    ws.addEventListener("error", reject, { once: true });
  });
  let id = 0;
  const waiting = new Map();
  ws.addEventListener("message", (event) => {
    const response = JSON.parse(event.data);
    const handler = waiting.get(response.id);
    if (!handler) return;
    waiting.delete(response.id);
    if (response.error) handler.reject(new Error(response.error.message));
    else handler.resolve(response.result);
  });
  const call = (method, params = {}) => {
    const next = ++id;
    const response = Promise.withResolvers();
    waiting.set(next, response);
    ws.send(JSON.stringify({ id: next, method, params }));
    return response.promise;
  };
  const evaluate = async (expression) => {
    const result = await call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
    return result.result.value;
  };
  await call("Page.enable");
  await call("Emulation.setDeviceMetricsOverride", { width: 1240, height: 800, deviceScaleFactor: 1, mobile: false });
  return {
    call, evaluate,
    async close() {
      ws.close();
      await (await fetch(new URL("/json/close/" + target.id, chrome))).text();
    },
    async navigate(url) {
      await call("Page.navigate", { url });
      await poll(() => evaluate("Boolean(document.getElementById('prompt') && document.getElementById('model')?.value)"), "chat composer and model");
    },
    async capture(file, theme) {
      await call("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: theme }] });
      await evaluate("new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))");
      const image = await call("Page.captureScreenshot", { format: "png" });
      await writeFile(join(root, file), Buffer.from(image.data, "base64"));
    },
  };
}

async function ask(page, text) {
  const result = await page.evaluate(`(() => {
    const input = document.getElementById("prompt");
    input.value = ${JSON.stringify(text)};
    input.dispatchEvent(new Event("input", { bubbles: true }));
    if (document.getElementById("send").disabled) return false;
    document.getElementById("send").click();
    return true;
  })()`);
  assert.equal(result, true);
}

const frontend = spawn(process.execPath, ["main.mjs"], {
  cwd: fileURLToPath(new URL("../", import.meta.url)),
  env: {
    PATH: process.env.PATH,
    GESANG_DEMO_GOAL_HOST_URL: host.url,
    GESANG_DEMO_AGENT_ID: host.agent,
    GESANG_DEMO_GRANT_ID: host.grant,
    GESANG_DEMO_GRANT_VERSION: String(host.version),
    GESANG_DEMO_OWNER_TOKEN_FILE: host.tokenFile,
    GESANG_DEMO_HOST_TIMEOUT_MS: "12000",
    GESANG_DEMO_MODELS: process.env.GESANG_TEST_MODEL_ID,
    GESANG_DEMO_PORT: "0",
  },
  stdio: ["ignore", "pipe", "inherit"],
});
const stopped = once(frontend, "exit");
let client;
try {
  const lines = createInterface({ input: frontend.stdout });
  const [ready] = await Promise.race([
    once(lines, "line"),
    stopped.then(() => { throw new Error("chat gateway did not start"); }),
  ]);
  const base = ready.split(" at ")[1];
  await writeFile(join(root, "web-url"), base);
  client = await page();
  await client.navigate(base);
  await client.evaluate(`window.fetch = ((actual) => async (resource, options) => {
    const response = await actual(resource, options);
    if (options?.method === "POST" && String(resource).endsWith("/messages")) {
      window.admission = { status: response.status, ...(await response.clone().json()) };
    }
    return response;
  })(window.fetch)`);
  const prompt = [
    "Write and run a short Python program with one command tool call.",
    "Pass argv as three strings: python3, -c, and the Python source code.",
    "Calculate sum(range(1, 101)) and write the number to result.txt using Python print with file=open, so it ends with a newline.",
    "Set the artifact argument to result.txt in that same call.",
    process.env.GESANG_TEST_MODEL_PROMPT_SUFFIX,
  ].filter(Boolean).join(" ");
  await ask(client, prompt);
  const admitted = await poll(() => client.evaluate("window.admission"), "submission");
  assert.equal(admitted.status, 202);
  assert.equal(admitted.state, "pending");
  assert.equal(admitted.id, admitted.requestId);
  await client.close();
  client = undefined;
  console.log(JSON.stringify({ event: "closed_after_admission", id: admitted.id }));
  const receiptPath = new URL(`v1/requests/${admitted.requestId}`, host.url);
  const authorization = { Authorization: "Bearer " + bearer };
  await poll(async () => {
    const receipt = await (await fetch(receiptPath, { headers: authorization })).json();
    if (receipt.state === "rejected") throw new Error("the original message was rejected");
    return receipt.state === "accepted";
  }, "durable execution receipt", operationTimeout, 500);
  const goalPath = new URL("v1/agents/" + host.agent + "/goals/" + admitted.id, host.url);
  const settled = await poll(async () => {
    const state = await (await fetch(goalPath, { headers: authorization })).json();
    if (["stopped", "failed", "waiting_user", "waiting_authorization"].includes(state.status)) {
      const snapshot = await (await fetch(base + "/api/conversations/" + admitted.id)).json();
      await writeFile(join(root, "unfinished.json"), JSON.stringify({ state, snapshot }, null, 2));
      throw new Error("goal settled without the expected publication: " + state.status);
    }
    return state.status === "completed" ? state : null;
  }, "worker completion with the browser closed", operationTimeout, 1000);
  assert.equal(settled.goalId, admitted.id);
  const publications = await (await fetch(goalPath.href + "/artifacts", { headers: authorization })).json();
  const publication = publications.artifacts.find((file) => file.name === "result.txt");
  assert.ok(publication);
  assert.equal(publication.sha256, createHash("sha256").update("5050\n").digest("hex"));
  assert.equal(publication.sizeBytes, 5);
  await writeFile(join(root, "publication.json"), JSON.stringify(publication, null, 2));
  console.log(JSON.stringify({ event: "worker_published_while_closed", id: admitted.id, publication }));
  client = await page();
  await client.navigate(base + "/?conversation=" + admitted.id);
  await poll(() => client.evaluate("Boolean(document.querySelector('#downloads a[download=\"result.txt\"]'))"), "published download link");
  const rendered = await client.evaluate("document.querySelector('#messages').innerText");
  const tools = await client.evaluate(
    "Array.from(document.querySelectorAll('#messages .message.tool .message-body'), (body) => body.textContent)",
  );
  assert(tools.some((output) => output.includes("Tool completed (exit code 0)")));
  await writeFile(join(root, "restored-messages.txt"), rendered);
  const downloadPath = join(root, "downloads");
  await mkdir(downloadPath, { mode: 0o700 });
  await client.call("Browser.setDownloadBehavior", { behavior: "allow", downloadPath });
  await client.evaluate("document.querySelector('#downloads a[download=\"result.txt\"]').click()");
  const downloaded = await poll(() => readFile(join(downloadPath, "result.txt")).catch(() => null), "Chrome download");
  assert.deepEqual(downloaded, Buffer.from("5050\n"));
  await client.capture("published-light.png", "light");
  await client.capture("published-dark.png", "dark");
  console.log(JSON.stringify({ event: "browser_download_verified", id: admitted.id, sha256: publication.sha256, bytes: downloaded.length }));
} finally {
  await client?.close();
  frontend.kill("SIGTERM");
  await stopped;
}
