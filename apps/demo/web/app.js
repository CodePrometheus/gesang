const copy = {
  untitled: "Untitled conversation",
  conversation: "Conversation",
  newChat: "New chat",
  loadingConversation: "Loading conversation…",
  noConversations: "No conversations yet.",
  historyUnavailable: "Could not load conversations.",
  noModels: "No models configured",
  modelsUnavailable: "Could not load models",
  modelRequired: "Configure a model to start a conversation.",
  loadingModels: "Loading models…",
  ready: "Ready",
  sending: "Sending message…",
  accepted: "Message accepted. Waiting for the conversation to update…",
  checking: "Checking the conversation…",
  running: "Gesang is working…",
  runFailed: "Gesang encountered an error. You can send another message.",
  reconnecting: "Connection lost. Reconnecting…",
  chatUnavailable: "Could not load the conversation. Reconnecting…",
  chatMissing: "This conversation could not be found.",
  createFailed: "Could not start a conversation.",
  sendFailed: "Could not send the message.",
  deliveryUnknown: "Delivery could not be confirmed. Check the conversation before sending again.",
  fileName: "Enter a single file name from /workspace/exports.",
  downloading: "Downloading…",
  downloadStarted: "Download started.",
  downloadFailed: "Could not download the file.",
  downloadFile: "Download",
  publishedFiles: "Files",
  rejected: "The original message was rejected.",
  roles: { user: "You", assistant: "Gesang", tool: "Tool result" },
};

const elements = Object.fromEntries(
  [
    "chat-title", "model-picker", "model", "history-toggle", "sidebar", "new-chat",
    "history-status", "conversations", "viewport", "welcome", "messages", "status",
    "retry", "send-error", "composer", "prompt", "send", "downloads", "artifact-form",
    "download-heading", "artifact-name", "download", "artifact-status",
  ].map((id) => [id, document.getElementById(id)]),
);

const views = new Map();
const pollIntervalMs = 1000;
let view;
let pollTimer;
let observation;
let renderedMessages = [];
let conversations = [];
let historyError = false;
let modelsError = false;
let modelsLoaded = false;
let modelsRequest;
let historyRequest;
let downloading = false;
let protectedHost = false;
let renderedFiles = "";
const files = document.createElement("ul");
files.className = "artifact-links";
elements.downloads.append(files);
files.hidden = true;

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

async function request(path, { body, signal, priority } = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    signal,
    priority,
    ...(body && {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => null);
    throw new ApiError(response.status, error?.error);
  }
  return response;
}

function showStatus(element, text, tone = "") {
  if (element.textContent !== text) element.textContent = text;
  element.dataset.tone = tone;
}

function errorMessage(prefix, error) {
  return error instanceof ApiError && error.message ? `${prefix} ${error.message}` : prefix;
}

function conversationPath(id) {
  return `/api/conversations/${encodeURIComponent(id)}`;
}

function conversationUrl(id) {
  const url = new URL(location.href);
  if (id === null) url.searchParams.delete("conversation");
  else url.searchParams.set("conversation", id);
  return url;
}

function restoreConversation() {
  selectConversation(new URLSearchParams(location.search).get("conversation") || null);
}

function selectConversation(id, navigate = false) {
  if (navigate && id !== view?.id) history.pushState(null, "", conversationUrl(id));
  if (view?.id === id) return;

  stopObservation();
  if (!views.has(id)) {
    views.set(id, {
      id,
      draft: "",
      snapshot: null,
      readError: null,
      sendError: "",
      writing: false,
      awaitingSnapshot: false,
      uncertainSubmission: null,
    });
  }
  view = views.get(id);
  elements.prompt.value = view.draft;
  renderedMessages = [];
  elements.messages.replaceChildren();
  renderConversation();
  elements.viewport.scrollTop = elements.viewport.scrollHeight;
  renderSelection();
  void observe(view);
}

function setHistoryOpen(open) {
  elements.sidebar.classList.toggle("is-open", open);
  elements["history-toggle"].setAttribute("aria-expanded", String(open));
}

function renderSelection() {
  const selected = conversations.find(({ id }) => id === view.id);
  elements["chat-title"].textContent = view.id === null
    ? copy.newChat
    : selected?.title || copy.conversation;
  elements["model-picker"].hidden = view.id !== null;
  for (const link of elements.conversations.querySelectorAll("a")) {
    if (link.dataset.id === view.id) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
}

async function loadModels() {
  if (modelsRequest) return modelsRequest;
  modelsRequest = (async () => {
    const select = elements.model;
    try {
      const configuration = await (await request("/api/models")).json();
      const { models } = configuration;
      if (protectedHost !== (configuration.protected === true)) {
        protectedHost = configuration.protected === true;
        renderedMessages = [];
        elements.messages.replaceChildren();
        renderConversation();
      }
      const previous = select.value;
      select.replaceChildren(...models.map((model) => new Option(model, model)));
      if (models.includes(previous)) select.value = previous;
      if (!models.length) select.add(new Option(copy.noModels, ""));
      modelsLoaded = true;
      modelsError = false;
    } catch {
      modelsError = true;
      if (select.disabled) select.replaceChildren(new Option(copy.modelsUnavailable, ""));
    } finally {
      modelsRequest = null;
      renderFeedback();
    }
  })();
  return modelsRequest;
}

async function loadHistory() {
  if (historyRequest) return historyRequest;
  historyRequest = (async () => {
    try {
      const data = await (await request("/api/conversations", { priority: "low" })).json();
      if (JSON.stringify(data.conversations) !== JSON.stringify(conversations)) {
        conversations = data.conversations;
        const list = document.createDocumentFragment();
        const focusedId = document.activeElement?.dataset.id;
        let focusedLink;
        for (const { id, title } of conversations) {
          const item = document.createElement("li");
          const link = document.createElement("a");
          link.href = conversationUrl(id);
          link.dataset.id = id;
          link.textContent = title || copy.untitled;
          link.title = link.textContent;
          if (id === focusedId) focusedLink = link;
          item.append(link);
          list.append(item);
        }
        elements.conversations.replaceChildren(list);
        focusedLink?.focus({ preventScroll: true });
      }
      historyError = false;
      elements["history-status"].textContent = conversations.length ? "" : copy.noConversations;
      elements["history-status"].hidden = Boolean(conversations.length);
      renderSelection();
    } catch {
      historyError = true;
      elements["history-status"].hidden = false;
      elements["history-status"].textContent = copy.historyUnavailable;
    } finally {
      historyRequest = null;
      renderFeedback();
    }
  })();
  return historyRequest;
}

function stopObservation() {
  clearTimeout(pollTimer);
  observation?.abort();
  observation = undefined;
}

async function observe(current) {
  if (current !== view || current.id === null || observation || document.hidden) return;
  clearTimeout(pollTimer);
  const controller = new AbortController();
  observation = controller;
  try {
    const uncertain = current.uncertainSubmission;
    const query = uncertain ? `?${new URLSearchParams({ receipt: uncertain.requestId })}` : "";
    const response = await request(conversationPath(current.id) + query, { signal: controller.signal });
    const snapshot = await response.json();
    if (controller.signal.aborted) return;
    const previous = current.snapshot;
    current.snapshot = snapshot;
    const receipt = snapshot.receipt;
    if (uncertain && uncertain === current.uncertainSubmission &&
        receipt?.requestId === uncertain.requestId && receipt.state !== "pending") {
      current.uncertainSubmission = null;
      if (receipt.state === "rejected") {
        current.sendError = copy.rejected;
      } else {
        if (current.draft === uncertain.text) {
          current.draft = "";
          elements.prompt.value = "";
        }
        current.sendError = "";
        if (receipt.id && receipt.id !== current.id) {
          selectConversation(receipt.id, true);
          void loadHistory();
          return;
        }
      }
    }
    if (snapshot.status !== "running" &&
        (current.awaitingSnapshot || previous?.status === "running")) {
      void loadHistory();
    }
    current.awaitingSnapshot = false;
    current.readError = null;
    renderConversation();
  } catch (error) {
    if (controller.signal.aborted) return;
    current.readError = error;
    renderFeedback();
  } finally {
    if (observation === controller) {
      observation = undefined;
      if (!document.hidden && current.readError?.status !== 404) {
        pollTimer = setTimeout(() => void observe(current), pollIntervalMs);
      }
    }
  }
}

function renderFeedback() {
  const select = elements.model;
  select.disabled = !modelsLoaded || !select.value || view.writing;
  const snapshot = view.snapshot;
  const canSend = view.id === null
    ? !select.disabled
    : snapshot && snapshot.status !== "running" && !view.readError &&
      (!protectedHost || snapshot.reply || view.uncertainSubmission?.first);
  const pending = snapshot?.receipt?.state === "pending";
  elements.send.disabled = !canSend || pending || view.writing || view.awaitingSnapshot || !view.draft.trim();
  elements.prompt.readOnly = view.writing;
  if (elements["send-error"].textContent !== view.sendError) {
    elements["send-error"].textContent = view.sendError;
  }
  elements.retry.hidden = !(view.readError || modelsError || historyError);

  let status = copy.ready;
  let tone = "";
  if (view.readError) {
    status = view.readError instanceof ApiError
      ? errorMessage(view.readError.status === 404 ? copy.chatMissing : copy.chatUnavailable, view.readError)
      : copy.reconnecting;
    tone = "error";
  } else if (view.writing) {
    status = copy.sending;
  } else if (pending) {
    status = copy.accepted;
  } else if (snapshot?.status === "running") {
    status = copy.running;
  } else if (view.awaitingSnapshot) {
    status = view.uncertainSubmission ? copy.checking : copy.accepted;
  } else if (snapshot?.status === "error") {
    status = copy.runFailed;
    tone = "error";
  } else if (snapshot?.waiting) {
    status = snapshot.waiting;
  } else if (view.id !== null && !snapshot) {
    status = copy.loadingConversation;
  } else if (view.id === null && select.disabled) {
    status = modelsError ? copy.modelsUnavailable : modelsLoaded ? copy.modelRequired : copy.loadingModels;
  }
  showStatus(elements.status, status, tone);
}

function renderConversation() {
  const snapshot = view.snapshot;
  const messages = snapshot?.messages ?? [];
  elements.welcome.hidden = view.id !== null && (
    messages.length > 0 || !snapshot || snapshot.status !== "idle"
  );
  let firstChanged = 0;
  while (firstChanged < renderedMessages.length && firstChanged < messages.length) {
    const previous = renderedMessages[firstChanged];
    const next = messages[firstChanged];
    if (previous.role !== next.role || previous.text !== next.text) break;
    firstChanged++;
  }
  if (firstChanged < renderedMessages.length || firstChanged < messages.length) {
    const viewport = elements.viewport;
    const atEnd = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight
      <= elements.send.offsetHeight;
    while (elements.messages.children.length > firstChanged) {
      elements.messages.lastElementChild.remove();
    }
    for (const message of messages.slice(firstChanged)) {
      elements.messages.append(renderMessage(message));
    }
    if (atEnd) viewport.scrollTop = viewport.scrollHeight;
    renderedMessages = messages;
  }
  renderFiles(snapshot?.artifacts);
  renderFeedback();
}

function renderFiles(artifacts = []) {
  elements["artifact-form"].hidden = protectedHost;
  elements.downloads.hidden = protectedHost && artifacts.length === 0;
  files.hidden = !protectedHost || artifacts.length === 0;
  if (!protectedHost) return;
  elements["download-heading"].textContent = copy.publishedFiles;
  const key = JSON.stringify([view.id, artifacts]);
  if (key === renderedFiles) return;
  renderedFiles = key;
  files.replaceChildren(...artifacts.map((file) => {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = `${conversationPath(view.id)}/artifacts/${encodeURIComponent(file.artifactId)}`;
    link.download = file.name;
    link.textContent = `${copy.downloadFile} ${file.name}`;
    item.append(link);
    return item;
  }));
  if (artifacts.length) elements.downloads.open = true;
}

function renderMessage({ role, text }) {
  const message = document.createElement("article");
  message.classList.add("message", role);
  const label = document.createElement("h2");
  label.textContent = copy.roles[role];
  const content = document.createElement(role === "tool" ? "pre" : "p");
  content.className = "message-body";
  content.textContent = text;
  if (role === "tool") {
    content.tabIndex = 0;
    content.setAttribute("aria-label", copy.roles.tool);
  }
  message.append(label, content);

  if (!protectedHost && role !== "user") {
    // Only explicit paths are linkified. The download form accepts other file names.
    const names = artifactNames(text);
    if (names.size) {
      const links = document.createElement("ul");
      links.className = "artifact-links";
      for (const name of names) {
        const item = document.createElement("li");
        const link = document.createElement("a");
        link.href = artifactPath(name);
        link.download = name;
        link.dataset.artifact = name;
        link.textContent = `${copy.downloadFile} ${name}`;
        item.append(link);
        links.append(item);
      }
      message.append(links);
    }
  }
  return message;
}

async function sendMessage(event) {
  event.preventDefault();
  if (elements.send.disabled) return;

  const original = view;
  let current = original;
  const text = original.draft;
  const model = elements.model.value;
  const previous = original.uncertainSubmission;
  const first = original.id === null || previous?.first === true;
  const retry = previous?.text === text || (protectedHost && previous?.first);
  const requestId = retry ? previous.requestId : crypto.randomUUID();
  const reply = retry ? previous.reply : original.snapshot?.reply;
  original.writing = true;
  original.sendError = "";
  original.uncertainSubmission = null;
  renderFeedback();

  try {
    if (current.id === null) {
      const id = protectedHost ? requestId
        : (await (await request("/api/conversations", { body: { model } })).json()).id;
      if (view !== original) {
        void loadHistory();
        return;
      }
      selectConversation(id, true);
      current = view;
      current.draft = text;
      original.draft = "";
      current.writing = true;
      current.sendError = "";
      current.uncertainSubmission = null;
      elements.prompt.value = text;
      renderFeedback();
      void loadHistory();
    }

    const response = await request(`${conversationPath(current.id)}/messages`, { body: { text, requestId, reply } });
    const receipt = await response.json();
    current.awaitingSnapshot = true;
    if (protectedHost && receipt.state === "rejected") {
      current.sendError = copy.rejected;
      return;
    }
    if (protectedHost && receipt.state === "pending") {
      current.uncertainSubmission = { text, requestId, reply, first };
      return;
    }
    current.draft = "";
    if (view === current) {
      elements.prompt.value = "";
      elements.prompt.focus();
      if (receipt.id && receipt.id !== current.id) selectConversation(receipt.id, true);
    }
  } catch (error) {
    if (current.id === null) {
      current.sendError = errorMessage(copy.createFailed, error);
    } else if (error instanceof ApiError && error.status < 500 && error.status !== 408) {
      current.sendError = errorMessage(copy.sendFailed, error);
    } else {
      current.sendError = errorMessage(copy.deliveryUnknown, error);
      current.uncertainSubmission = { text, requestId, reply, first };
      current.awaitingSnapshot = true;
    }
  } finally {
    original.writing = false;
    current.writing = false;
    renderFeedback();
    if (view === current) {
      // Read again after the POST so an older snapshot cannot settle this submission.
      stopObservation();
      void observe(current);
    }
  }
}

function artifactNames(text) {
  const paths = /(?<![\w/.-])(?:([`"'])\/workspace\/exports\/([^/\r\n]+?)\1|\/workspace\/exports\/([\w.-]+?)(?=[.!?]?(?:$|[\s<>,;:\])}])))/g;
  return new Set(Array.from(text.matchAll(paths), (match) => match[2] ?? match[3]).filter(isFileName));
}

function isFileName(name) {
  return Boolean(name) && name !== "." && name !== ".." &&
    !/[\\/\x00-\x1f\x7f]/.test(name);
}

function artifactPath(name) {
  return `/api/artifacts/${encodeURIComponent(name)}`;
}

async function downloadArtifact(name) {
  const status = elements["artifact-status"];
  if (downloading) return;
  if (!isFileName(name)) {
    showStatus(status, copy.fileName, "error");
    return;
  }

  downloading = true;
  elements.download.disabled = true;
  showStatus(status, copy.downloading);
  try {
    const response = await request(artifactPath(name));
    const url = URL.createObjectURL(await response.blob());
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    document.body.append(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
    showStatus(status, copy.downloadStarted, "success");
  } catch (error) {
    showStatus(status, errorMessage(copy.downloadFailed, error), "error");
  } finally {
    downloading = false;
    elements.download.disabled = false;
  }
}

function plainClick(event) {
  return event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey;
}

function reconnect() {
  void loadModels();
  void loadHistory();
  void observe(view);
}

elements.conversations.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-id]");
  if (!link || !plainClick(event)) return;
  event.preventDefault();
  selectConversation(link.dataset.id, true);
  setHistoryOpen(false);
  if (getComputedStyle(elements["history-toggle"]).display !== "none") {
    elements["history-toggle"].focus();
  }
});
elements["history-toggle"].addEventListener("click", () => {
  setHistoryOpen(elements["history-toggle"].getAttribute("aria-expanded") === "false");
});
elements["new-chat"].addEventListener("click", () => {
  selectConversation(null, true);
  setHistoryOpen(false);
  elements.prompt.focus();
});

elements.prompt.addEventListener("input", () => {
  view.draft = elements.prompt.value;
  view.sendError = "";
  renderFeedback();
});
elements.prompt.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" || event.shiftKey || event.isComposing || event.keyCode === 229) return;
  event.preventDefault();
  if (!event.repeat && !elements.send.disabled) elements.composer.requestSubmit(elements.send);
});
elements.model.addEventListener("change", renderFeedback);
elements.composer.addEventListener("submit", sendMessage);
elements.retry.addEventListener("click", reconnect);

elements["artifact-form"].addEventListener("submit", (event) => {
  event.preventDefault();
  void downloadArtifact(elements["artifact-name"].value);
});
elements.messages.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-artifact]");
  if (!link || !plainClick(event)) return;
  event.preventDefault();
  elements.downloads.open = true;
  elements["artifact-name"].value = link.dataset.artifact;
  void downloadArtifact(link.dataset.artifact);
});

window.addEventListener("popstate", restoreConversation);
window.addEventListener("online", reconnect);
document.addEventListener("visibilitychange", () => {
  if (document.hidden) stopObservation();
  else reconnect();
});

restoreConversation();
reconnect();
