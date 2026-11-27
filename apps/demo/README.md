# Gesang standalone demo

Open a chat, choose a model, and give Gesang a job. The model runs through
Chopin. Its tools run in a Linux Docker workspace, and the browser can read
command results and download the files the agent writes to `/workspace/exports`.

## Start on your server

Use Node.js 26.1 or later, npm, Docker, and a model endpoint that supports
the OpenAI chat completions API with tool calls. Run these commands as the
operator of the demo on your server:

```sh
cd apps/demo
npm ci
npm run build:workspace
```

Set `GESANG_DEMO_MODEL_URL` to the API base URL, such as
`https://provider.example/v1`. Set `GESANG_DEMO_MODELS` to the comma-separated
list of model IDs enabled at that endpoint. For a foreground run, set
`GESANG_DEMO_MODEL_API_KEY` in the server environment or set
`GESANG_DEMO_MODEL_API_KEY_FILE` to a private file containing the key. If both
are set, Gesang uses the file. Model credentials stay with the Gesang process.

Start the service with `npm start`. Its default address is
`http://127.0.0.1:8787`. On another machine, forward that port over SSH:

```sh
ssh -N -L 8787:127.0.0.1:8787 user@server
```

Open `http://127.0.0.1:8787` in your browser. Try this prompt:

> Write a Python script that prints the sum of 1 through 100. Run it and
> save its output as exports/result.txt. Tell me the file name when done.

After the agent finishes, the chat shows the tool output. Download
`result.txt` from the workspace. You can close the browser and reopen the
same conversation while the server is running.
A service restart can interrupt a shell command. The conversation records
that outcome and retains files already written to the work volume.

## Keep Gesang running with systemd

On a Linux server with systemd, place the contents of `apps/demo` in
`~/.local/share/gesang-demo/`. In that directory, run `npm ci` and
`npm run build:workspace` as the account that will run the service. That
account needs Docker access. Install Node.js where the service can find it,
such as `~/.local/bin` or `/usr/local/bin`.

```sh
install -d -m 700 ~/.config/gesang-demo
install -d ~/.config/systemd/user
install -m 644 gesang-demo.service ~/.config/systemd/user/
```

Create `~/.config/gesang-demo/settings.env` with the model endpoint and model
IDs. Add any of the optional settings below as `NAME=value` lines. Store the
API key in `~/.config/gesang-demo/model-api-key` and set that file's permissions
to `0600`. The service loads it using systemd's credential directory.

```sh
systemctl --user daemon-reload
systemctl --user enable --now gesang-demo.service
journalctl --user -u gesang-demo.service -n 30
```

If `loginctl show-user "$USER" -p Linger` reports `Linger=no`, have a server
administrator enable lingering for this account so the service starts on boot
and keeps running after the last SSH session closes. Connect over the SSH
forwarding command above. Conversations live in the user state directory
(`~/.local/state/gesang-demo` by default) and files live in a Docker named
volume.

## Attach a Kubernetes sandbox

Gesang can also attach to a workspace provisioned by an existing Kubernetes
Agent Sandbox controller. Build the application image with
`docker build -f service.Dockerfile -t gesang-demo-service .`. Run one replica
with a persistent state mount and an authenticated operator connection to
the HTTP listener.

In the workspace, run Chopin's `loopback-listener.sh 7070 /usr/local/bin/pi-env`
in a non-root container with the work volume at `/workspace`. Put a mutually
authenticated TLS relay in a separate container of the same Pod. The relay
forwards to `127.0.0.1:7070` and mounts only its TLS material. Allow access
to the relay port from the Gesang service Pod only, and give the workspace
no outbound network access. Use a dedicated CA and a relay certificate whose
DNS SAN matches its service name. Only the Gesang Pod receives the client
certificate and model credential.

The [Kustomize base](deploy/kubernetes/) describes the service and workspace
resources. Use an overlay to set the namespace, pinned image references,
storage class, resource limits, and the sandbox runtime class for your cluster.
The workspace image must include the existing Chopin loopback listener, pi-env
daemon, and Python 3. Use an existing compatible image or run
`npm run build:sandbox-workspace` and publish it to your registry. The sample
relay uses socat, which can also come from a separate pinned image.

Create a `gesang-settings` ConfigMap containing the model endpoint, model IDs,
sandbox host, and TLS port. The service expects Secrets named `gesang-model`
(with key `api-key`), `gesang-client-tls`, and `gesang-server-tls`. Each TLS
Secret contains `ca.crt`, `tls.crt`, and `tls.key`. Keep the service's HTTP
listener private and connect via an authenticated channel, such as port
forwarding to the service Pod. Keep workspace administration subject to
your platform's entry policy.

Set the three sandbox settings below together. Gesang verifies the relay
certificate, opens the existing pi-env protocol connection, and reads
exports through that connection. The platform provisions and stops the
Sandbox and owns the workspace volume.

## Message receipts

### Attach to a protected execution host

The same chat can use an already configured Gesang host that mounts
`CommandGateway` and `GoalGateway` and runs kernel recovery. The host supplies
the owner, task delegation, execution model, and isolated workspace.

| Environment variable | Value |
| --- | --- |
| `GESANG_DEMO_GOAL_HOST_URL` | Private HTTPS origin or loopback HTTP URL for the host's API root. An optional path prefix is preserved. |
| `GESANG_DEMO_AGENT_ID` | The owner's configured agent ID. |
| `GESANG_DEMO_GRANT_ID` | ID of the current task grant. |
| `GESANG_DEMO_GRANT_VERSION` | Exact version of that grant. |
| `GESANG_DEMO_OWNER_TOKEN_FILE` | Private file with an owner credential scoped to command submission, receipt lookup, and goal observation for this agent. |
| `GESANG_DEMO_HOST_TIMEOUT_MS` | Deadline for one request to the protected host, including a download. |
| `GESANG_DEMO_HISTORY_LIMIT` | Number of recent goals in the sidebar. Defaults to 40. |

Set `GESANG_DEMO_MODELS` to the execution model configured on that host. Start the
service with `npm start` and access the loopback listener over the authenticated
tunnel. Model API keys and workspace provisioning stay with the protected host.
The owner token is loaded by the demo service and does not reach the browser.
The first message opens a durable goal. A reply to a pending question targets
that question. A follow-up after the goal settles continues its conversation
under a new goal ID. Published files are available from their original conversation.

### Receipt API

Clients can send a lowercase UUIDv4 as `requestId` in
`POST /api/conversations/:id/messages`. A successful response includes the
original `requestId` and `submissionId`. Resending the same text with the same
ID returns the existing receipt. Changing the text under an existing ID
returns HTTP 409. Legacy requests with only `text` continue to work.

To check an uncertain delivery, add `?receipt=<requestId>` to
`GET /api/conversations/:id`. The snapshot includes the original receipt or
`null` if it has yet to be admitted. A receipt confirms admission. The
conversation shows subsequent execution and results.

## Settings

| Environment variable | Value |
| --- | --- |
| `GESANG_DEMO_PORT` | Loopback port, defaults to `8787`. |
| `GESANG_DEMO_DATA_DIR` | Conversation data directory, defaults to `~/.local/state/gesang-demo`. |
| `GESANG_DEMO_WORKSPACE_IMAGE` | Docker image, defaults to `gesang-demo-workspace:local`. |
| `GESANG_DEMO_SANDBOX_HOST` | Optional DNS name of the provisioned workspace's TLS relay. |
| `GESANG_DEMO_SANDBOX_PORT` | TLS relay port when attaching a provisioned sandbox. |
| `GESANG_DEMO_SANDBOX_TLS_DIR` | Private directory containing the deployment CA (`ca.crt`) and the client certificate and key (`tls.crt`, `tls.key`). |
| `GESANG_DEMO_WORKSPACE_CPUS` | Optional workspace CPU limit. |
| `GESANG_DEMO_WORKSPACE_MEMORY` | Optional Docker memory limit, for example `768m`. |
| `GESANG_DEMO_WORKSPACE_PIDS` | Optional workspace process limit. |
| `GESANG_DEMO_MAX_OUTPUT_TOKENS` | Optional positive output-token limit per model request. |
| `GESANG_DEMO_MAX_TOKENS_FIELD` | Optional request field for that limit. Set `max_tokens` for Ollama or use `max_completion_tokens` where supported. |
| `GESANG_DEMO_REASONING_OFF_VALUE` | Optional value for providers that need an explicit `reasoning_effort` to disable thinking. Ollama accepts `none` for supported models. |

The standalone demo is for one operator with access to the host. Keep the HTTP
listener on loopback and use an authenticated SSH tunnel from another machine.
Store conversation data in a private directory. The separate Docker workspace
has its own volume. Use one Gesang process per data directory.
