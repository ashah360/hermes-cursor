# ghost_cursor

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) plugin that lets your agent **delegate coding tasks to the [Cursor](https://cursor.com) agent** — and watch it work in real time.

Registers seven session tools that run Cursor **cloud agents** against a target repo over the **Cursor REST v1 API + SSE event stream** — on this machine (`runtime="local"`, a plugin-managed "My Machines" worker) or on a cursor-hosted VM (`runtime="cloud"`) — stream per-edit progress (reasoning + full file diffs) back through the calling agent's progress callback, and return a structured summary of everything that changed.

Because it's an ordinary Hermes tool call inside a real session, the result **persists in the transcript and reloads for free** — and interrupts map to a native `run.cancel()`.

## Why REST + SSE

Earlier versions drove `cursor-agent` over ACP (JSON-RPC on stdio), scraped `--print` stdout, and then rode the python `cursor-sdk` (which spawned a local sidecar bridge). The REST v1 API replaces all of them with a direct, supported contract:

| | stdout scraping | ACP (v0.4) | cursor-sdk (v0.5) | REST + SSE (this plugin) |
|---|---|---|---|---|
| Event format | freeform JSON, inferred | typed `session/update` | typed `SDKMessage` | typed SSE events (`assistant` / `tool_call` / `result`) |
| Cancellation | `kill -9` the process | native `session/cancel` | native `run.cancel()` | `POST …/runs/{id}/cancel` |
| Resume | none | `session/load` (best effort) | `Agent.resume` | follow-up `POST …/agents/{id}/runs` |
| Network drop mid-run | run lost | run lost | `run.observe` re-attach | `Last-Event-ID` reconnect |
| Local process state | one process per run | one process per run | sidecar bridge to babysit | **none** — agent state lives server-side |

Because agent state lives entirely on Cursor's side, a dropped stream — or a restarted plugin process — re-attaches to a run that is still executing instead of losing it, and there is no bridge process to go stale. Terminal truth comes from the `result` SSE event confirmed by a final `GET runs/{id}` — never from the lossy simplified status stream.

The legacy `--print` runner is kept in `runner.py` as a reference/fallback.

## What you get (v0.6 — cloud-machine runtime)

Seven named-session tools. The handle is a **meaningful session title** (e.g. `Fix payment webhook retries`) provided by the caller to `cursor_create_session`; cursor agent ids from older runs resolve as aliases.

- **`cursor_create_session(title?, repo?, model?, runtime?)`** — registers a named session and dispatches **nothing** (the cloud agent is created lazily on the first message). `title` is a concise phrase (roughly 3–8 words, plain words with spaces, max 80 chars) describing the task; it becomes the handle everywhere, including the agent's name on cursor.com. A taken title fails the create with the existing entry's status and age; an omitted title falls back to `<repo-basename> session` (numeric suffix past collisions). `runtime="local"` (default) routes execution to a plugin-managed "My Machines" worker on this machine; `runtime="cloud"` uses a cursor-hosted VM. `repo` is a local path either way — the plugin derives the GitHub origin URL + branch itself.
- **`cursor_send_message(session, message, ...)`** — all work goes through this. The first message on a fresh session is the task; later messages are follow-ups with full prior context. Honest semantics: there is **no mid-run queue** — sending into a live run cancels it natively and re-prompts the same session. Returns immediately; the run executes in the background and the final result is delivered as a message on every terminal state.
- **`cursor_status(session)`** — **strictly read-only**: status, elapsed, last-activity age, files changed so far, recent events. Polling never cancels (tested property).
- **`cursor_events(session, offset?, limit?, kind?)`** — pages the per-session JSONL event log (reasoning, tool calls, file diffs, content).
- **`cursor_stop(session)`** — native cancel; acks "stopped" only after the run is observed terminal.
- **`cursor_list(scope?)`** — TSV of session handles with repo, runtime, and status.
- **`cursor_subscribe(session, interval_s)`** — subscribe the **calling Hermes session** to periodic progress digests while a run is active. Subscriptions are per Hermes session per cursor session — multiple Hermes sessions can watch one run, each at its own cadence, each getting its own copy of every digest **and** the completion. `cursor_send_message` auto-subscribes the caller (explicit `update_interval_s` param > persisted interval > 180s default, `0` disables); `interval_s=0` removes only the caller's subscription. The final result is always delivered to every subscriber — and to the dispatching session even if it unsubscribed.

Cross-cutting: **live streaming** (reasoning + per-edit `file_diff`s via the agent's `tool_progress_callback`), **completion delivery** on every terminal state, **same-repo concurrency guard** (a second run on a repo with an active run is rejected; different repos run in parallel), **handle persistence** across restarts (a JSON table under `<HERMES_HOME>/state/`), and a **`check_fn`** so the tools only appear when the transport is available.

## The worker model (runtime="local")

Sessions are real cloud agents either way — the agent loop always runs on Cursor's side, which is exactly why every session **syncs natively to cursor.com/agents, the web app, and mobile**. With `runtime="local"`, tool calls (terminal, edits) execute on this machine through a "My Machines" worker:

- **Canonical identity**: every local repo input resolves through `realpath(git rev-parse --show-toplevel)` — `/repo` and `/repo/subdir` are ONE worker + admission identity; sibling linked git worktrees of the same repository stay distinct. Names are `<hostname>-<8-char path hash>`.
- **One worker per worktree, fully isolated**: each worker gets a deterministic private `CURSOR_DATA_DIR` under the state dir (the CLI's exclusive `worker.lock` lives inside it, so parallel worktrees never contend) plus a localhost `--management-addr` health/readiness endpoint. Cursor authentication stays shared.
- **Supervision**: workers run as deterministic **transient user systemd services** (`cursor-worker-<name>.service`) — gateway-independent lifetime, MainPID/InvocationID as the record's generation fence, bounded full-cgroup stop, adopted across gateway restarts. Without a user manager the spawn degrades (clearly surfaced) to a detached process with bounded process-group teardown. **Gateway restarts don't kill runs or workers.**
- **Per-run leases**: a run leases its worker from dispatch until the observed remote-terminal settle; the reconciler reaps only **leaseless** workers past an idle TTL (`GHOST_CURSOR_WORKER_IDLE_TTL_S`, default 1800). At the worker cap (`GHOST_CURSOR_MAX_WORKERS`, default 10 = Cursor's documented per-user quota) idle workers are reclaimed first; an all-leased fleet fails with an honest capacity error, never silent serialization.
- Routing match is threefold: the worker belongs to the API key's cursor account, the name matches, and the worker's registered repo (its checkout's git origin) matches the target. No match → the create is rejected server-side, no silent fallback to a cursor-hosted VM.

## Requirements

- [Hermes Agent](https://github.com/NousResearch/hermes-agent)
- `httpx` importable (already a Hermes dependency)
- the `agent` CLI on PATH for `runtime="local"` (`curl https://cursor.com/install -fsS | bash`) — the plugin spawns and manages the detached worker for you
- `CURSOR_API_KEY` exported (create one at the [Cursor dashboard](https://cursor.com/dashboard))
- The target repo should be a git repo (enables the diff fallback)

## Install

Drop the plugin into your Hermes plugins directory and enable it:

```bash
# 1. copy the plugin
mkdir -p ~/.hermes/plugins/ghost_cursor
cp __init__.py rest_client.py cloud_runner.py workers.py progress.py events.py runner.py jobs.py handles.py eventlog.py render.py plugin.yaml ~/.hermes/plugins/ghost_cursor/

# 2. enable it in ~/.hermes/config.yaml
#    plugins:
#      enabled:
#        - ghost_cursor

# 3. restart the gateway so the tools load
hermes gateway restart
```

Verify it registered:

```bash
# cursor_create_session / cursor_send_message / cursor_status / cursor_stop /
# cursor_events / cursor_list / cursor_subscribe should show up as tools once
# httpx is importable
```

## Usage

In practice you just talk to your agent normally — when a task is coding work, it creates a session and dispatches:

> "Add a `subtract(a, b)` function to `calc.py`"

```
cursor_create_session(repo="/path/to/repo",
                      title="Add subtract to calc")   → session: Add subtract to calc
cursor_send_message("Add subtract to calc", task)     → runs in background
  … completion auto-delivers with a summary + files changed + diffs
```

Follow-ups go to the same name and keep full prior context:

```
cursor_send_message("Add subtract to calc", "now add divide in the same style")
```

Because the session is a real cloud agent, it also appears in Cursor's Agents UI (web + mobile) — you can open the same conversation there and watch or continue it.

## Steering a running task

Cursor has no true mid-prompt queue — a second prompt cancels and replaces the current one. So sending into a live run is an honest **interrupt + re-prompt with context**: the in-flight step is discarded (the ack says so), and the run continues from your new instruction with everything it already knew. Work already written to the tree survives.

## Timeouts — inactivity, not wall clock

Timeouts are **inactivity-based**: a run that keeps streaming events (reasoning, tool calls, content) is alive and is never killed for total elapsed time. Only a *silent* run is treated as hung.

- **`inactivity_timeout_s`** — abort after this many seconds with **no stream events**; any streamed activity resets the clock. Default **600** (10 min of silence); **0 disables** the watchdog.
- **`max_wall_s`** — optional hard ceiling on **total** run time, a safety net for runaways that stream forever without finishing. Default **0 (disabled)**.

Precedence for both: explicit tool param → config.yaml (`plugins.ghost_cursor.inactivity_timeout_s` / `plugins.ghost_cursor.max_wall_s`) → built-in default. The abort error names whichever limit fired ("no activity for Ns" vs "exceeded max wall time (Ns)"), and either one delivers a normal `timeout` completion message. `cursor_status` reports `last_activity_s` (seconds since the last stream event) so you can spot a run going quiet **before** the watchdog fires — it's advisory only; the enforcement lives in the cloud runner.

The old `timeout` parameter is kept as a deprecated alias for `inactivity_timeout_s`.

## How live progress works (no core patch)

A registry-dispatched tool handler isn't handed the calling `AIAgent`, but Hermes installs the agent's `_touch_activity` as a thread-local activity callback right before each tool dispatch. `_resolve_progress_callback()` reads that thread-local, walks `__self__` back to the live agent, and uses its `tool_progress_callback`. Each emission is:

```
tool_progress_callback("reasoning.available", "cursor_edit", <json-envelope>, None)
```

which the api_server session-chat-stream forwards mid-turn as `event: tool.progress`. The JSON `delta` is a canonical envelope — `content` / `tool_use` / `tool_result` / `lifecycle` / `file_diff` — that a UI keys on (`tool_name == "cursor_edit"`, `source: "ghost"`) to render live tool cards and diffs.

## Files

| File | Role |
|---|---|
| `__init__.py` | Plugin entry — registers the seven tools, resolves the progress callback, builds results |
| `progress.py` | Progress subscriptions — per-run digest timers, `cursor_subscribe` plumbing, completion-queue delivery guards |
| `rest_client.py` | Typed httpx client for the Cursor REST v1 API — error mapping, GET-only retries, SSE parser |
| `cloud_runner.py` | REST+SSE transport — agent create/follow-up, SSE consumption with Last-Event-ID re-attach, watchdogs, native cancel, terminal settle via GET runs/{id} |
| `workers.py` | "My Machines" worker controller for runtime=local — canonical worktree identity, isolated data roots, systemd transient services (degraded detached fallback), generation-fenced records, per-run leases, idle reaping, capacity |
| `events.py` | Canonical envelope builders + cloud-event → envelope mapping |
| `jobs.py` | Background job tracking + completion/digest delivery into the agent loop |
| `handles.py` | Session-handle persistence (name → agent id, repo, runtime, subscribers) |
| `eventlog.py` | Per-session JSONL event log |
| `render.py` | Plain-text rendering for tool outputs |
| `runner.py` | Legacy `--print` stdout runner (reference/fallback) + shared helpers |
| `plugin.yaml` | Plugin manifest |
| `test_ghost_cursor_plugin.py` | Tests |

## License

MIT — see [LICENSE](LICENSE).

## CI

Three GitHub Actions workflows (`.github/workflows/`):

- **`unit`** (every push/PR, blocking) — the hermetic unit tests against real Hermes-core imports. Fast, deterministic, no secrets, no network.
- **`e2e-test`** / **`e2e-eval`** (**manual dispatch only**) — the real deal: **no mocks**. They install real Hermes and drive the real Cursor REST API end to end (every handle shape, plus an LLM-as-judge quality pass). Every run creates **real cloud agents against the `CURSOR_API_KEY` account** — real usage cost, real sessions in that account's Agents UI — so they never fire automatically. Trigger from the Actions tab (or `gh workflow run e2e-test`) before releases or after transport changes. Assertions are **invariants** ("a `.py` exists / imports / `add(2,3)==5` / status never cancels the run"), never exact diffs — the model is nondeterministic.

Set the repo secret **`CURSOR_API_KEY`** for the e2e jobs — the account must have **private workers enabled** (an account can register workers yet still 403 on machine-routed agent creation; that split is the entitlement signature, not a code bug). Pin the CI model via the `GHOST_CURSOR_TEST_MODEL` env (default `gpt-5.4-nano` — cheap + fast; verify the slug against `GET /v1/models`). Set the **`GHOST_CURSOR_E2E_REPO`** repository variable to a GitHub repo the key's GitHub connection can see (cursor verifies the branch server-side at create).

Reproduce the e2e env locally with `Dockerfile.e2e`:

```bash
docker build -t ghost-cursor-e2e -f Dockerfile.e2e .
docker run --rm -e CURSOR_API_KEY=sk-... ghost-cursor-e2e
```
