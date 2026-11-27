import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const HOST = "127.0.0.1";
const MAX_JSON_BYTES = 2 * 1024 * 1024;
const STATIC_FILES = new Map([
  ["/", ["index.html", "text/html"]],
  ["/index.html", ["index.html", "text/html"]],
  ["/app.js", ["app.js", "text/javascript"]],
  ["/styles.css", ["styles.css", "text/css"]],
]);

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function sendJson(response, status, body) {
  const json = JSON.stringify(body);
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  response.end(json);
}

function localOrigin(request) {
  const [host, duplicate] = request.headersDistinct.host ?? [];
  if (duplicate !== undefined || !/^(?:localhost|127\.0\.0\.1|\[::1\])(?::[0-9]+)?$/i.test(host)) {
    throw new HttpError(403, "Use a loopback address to access the demo.");
  }
  try {
    return new URL(`http://${host}`).origin;
  } catch {
    throw new HttpError(400, "Invalid Host header.");
  }
}

function verifyBrowserOrigin(request, expectedOrigin, allowNavigation = false) {
  const { origin, "sec-fetch-site": site } = request.headers;
  // Browsers send origin or fetch metadata. Native clients can send JSON directly.
  if (
    (origin !== undefined && origin !== expectedOrigin) ||
    (site !== undefined && site !== "same-origin" && !(allowNavigation && site === "none"))
  ) {
    throw new HttpError(403, "Cross-origin requests are forbidden.");
  }
}

async function readJson(request, expectedOrigin) {
  verifyBrowserOrigin(request, expectedOrigin);
  const [contentType, duplicate] = request.headersDistinct["content-type"] ?? [];
  const mediaType = contentType?.split(";", 1)[0].trim().toLowerCase();
  if (duplicate !== undefined || mediaType !== "application/json") {
    throw new HttpError(415, "Content-Type must be application/json.");
  }
  if (Number(request.headers["content-length"]) > MAX_JSON_BYTES) {
    throw new HttpError(413, "JSON request body exceeds 2 MiB.");
  }

  let size = 0;
  const chunks = [];
  // Keep the socket open long enough to send 413 when a streamed body exceeds the limit.
  for await (const chunk of request.iterator({ destroyOnReturn: false })) {
    size += chunk.length;
    if (size > MAX_JSON_BYTES) {
      throw new HttpError(413, "JSON request body exceeds 2 MiB.");
    }
    chunks.push(chunk);
  }

  let body;
  try {
    body = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, size)));
  } catch {
    throw new HttpError(400, "Request body must contain valid UTF-8 JSON.");
  }
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new HttpError(400, "Request body must be a JSON object.");
  }
  return body;
}

function requireMethod(request, response, ...methods) {
  if (!methods.includes(request.method)) {
    response.setHeader("Allow", methods.join(", "));
    throw new HttpError(405, "Method not allowed.");
  }
}

function decodeSegment(segment) {
  let value;
  try {
    value = decodeURIComponent(segment);
  } catch {
    throw new HttpError(400, "Invalid path segment encoding.");
  }
  if (value === "." || value === ".." || /[\/\\\x00-\x1f\x7f]/.test(value)) {
    throw new HttpError(400, "Invalid path segment.");
  }
  return value;
}

function attachment(name) {
  if (/^[A-Za-z0-9._-]+$/.test(name)) {
    return `attachment; filename="${name}"`;
  }
  const encoded = encodeURIComponent(name).replace(/['()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
  return `attachment; filename*=UTF-8''${encoded}`;
}

function verifyRequestId(value) {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(value)) {
    throw new HttpError(400, "Request ID must be a lowercase UUIDv4.");
  }
  return value;
}

async function serve(request, response, runtime, modelIds) {
  const origin = localOrigin(request);
  const path = request.url.split("?", 1)[0];
  if (runtime.protected && request.method === "GET" && path.startsWith("/api/")) {
    verifyBrowserOrigin(request, origin, true);
  }
  const asset = STATIC_FILES.get(path);
  if (asset) {
    requireMethod(request, response, "GET", "HEAD");
    const [filename, type] = asset;
    const bytes = await readFile(new URL(`./web/${filename}`, import.meta.url));
    response.writeHead(200, {
      "Content-Type": `${type}; charset=utf-8`,
      "Content-Length": bytes.byteLength,
    });
    return response.end(bytes);
  }

  if (path === "/api/models") {
    requireMethod(request, response, "GET");
    return sendJson(response, 200, { models: modelIds, ...(runtime.protected && { protected: true }) });
  }
  if (path === "/api/conversations") {
    requireMethod(request, response, "GET", ...(runtime.protected ? [] : ["POST"]));
    if (request.method === "GET") {
      return sendJson(response, 200, { conversations: await runtime.listConversations() });
    }
    const { model } = await readJson(request, origin);
    if (typeof model !== "string" || !modelIds.includes(model)) {
      throw new HttpError(400, "Select a configured model.");
    }
    return sendJson(response, 201, await runtime.createConversation({ model }));
  }

  const published = /^\/api\/conversations\/([^/]+)\/artifacts\/([^/]+)$/.exec(path);
  if (published && runtime.protected) {
    requireMethod(request, response, "GET");
    const controller = new AbortController();
    response.once("close", () => controller.abort());
    const [, id, artifactId] = published;
    const file = await runtime.readPublishedArtifact(decodeSegment(id), decodeSegment(artifactId), controller.signal);
    response.setHeader("Content-Type", "application/octet-stream");
    for (const name of ["Content-Disposition", "Content-Digest"]) {
      const value = file.headers.get(name);
      if (value !== null) response.setHeader(name, value);
    }
    await pipeline(Readable.fromWeb(file.body), response);
    return;
  }

  const conversation = /^\/api\/conversations\/([^/]+)(\/messages)?$/.exec(path);
  if (conversation) {
    const [, segment, messages] = conversation;
    requireMethod(request, response, messages ? "POST" : "GET");
    const id = decodeSegment(segment);
    if (messages) {
      const { text, requestId, reply } = await readJson(request, origin);
      if (typeof text !== "string" || !text.trim()) {
        throw new HttpError(400, "Message text is required.");
      }
      const receipt = await runtime.postMessage(id, text, verifyRequestId(requestId), reply);
      const status = runtime.protected && receipt.state !== "pending" ? 200 : 202;
      return sendJson(response, status, receipt);
    }
    const receiptId = new URLSearchParams(request.url.slice(path.length + 1)).get("receipt") ?? undefined;
    const snapshot = await runtime.getConversation(id, verifyRequestId(receiptId));
    if (snapshot == null) {
      throw new HttpError(404, "Conversation not found.");
    }
    return sendJson(response, 200, snapshot);
  }

  const artifact = /^\/api\/artifacts\/([^/]+)$/.exec(path);
  if (artifact && !runtime.protected) {
    requireMethod(request, response, "GET");
    const name = decodeSegment(artifact[1]);
    const bytes = await runtime.readArtifact(name);
    if (bytes == null) {
      throw new HttpError(404, "Artifact not found.");
    }
    if (!(bytes instanceof Uint8Array)) {
      throw new TypeError("The runtime must return artifact bytes.");
    }
    response.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Disposition": attachment(name),
      "Content-Length": bytes.byteLength,
    });
    return response.end(bytes);
  }

  throw new HttpError(404, "Route not found.");
}

/** Listen on loopback. Port zero selects a free port. The caller owns runtime shutdown. */
export async function startServer({ runtime, modelIds, port }) {
  const server = createServer(async (request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
    response.setHeader("X-Content-Type-Options", "nosniff");
    try {
      await serve(request, response, runtime, modelIds);
    } catch (error) {
      if (response.destroyed) return;
      if (response.headersSent) {
        response.destroy();
        return;
      }
      if (!request.complete) response.setHeader("Connection", "close");
      if (error instanceof HttpError) {
        sendJson(response, error.status, { error: error.message });
      } else if (error?.statusCode === 400) {
        sendJson(response, 400, { error: "The runtime rejected the request." });
      } else if (error?.statusCode === 409) {
        sendJson(response, 409, { error: "The request ID already belongs to a different message." });
      } else if (error?.statusCode === 404) {
        sendJson(response, 404, { error: "The requested resource was not found." });
      } else if (error?.statusCode === 403 || error?.statusCode === 401) {
        sendJson(response, 403, { error: "The protected host refused the operation." });
      } else if (error?.statusCode === 413) {
        sendJson(response, 413, { error: "The request exceeds the configured limit." });
      } else if (error?.statusCode === 503) {
        sendJson(response, 503, { error: "The protected host is temporarily unavailable." });
      } else {
        sendJson(response, 500, { error: "The demo could not complete the request." });
      }
    }
  });

  server.listen(port, HOST);
  await once(server, "listening");
  let closing;
  return {
    url: `http://${HOST}:${server.address().port}`,
    close() {
      closing ??= new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      return closing;
    },
  };
}
