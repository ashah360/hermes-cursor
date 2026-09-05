# Codex backend — design note

Scope: add a Codex app-server backend beside the existing Cursor backend
without changing any `cursor_*` tool, Cursor session record, or Cursor
worker behavior. Full authoritative scope: the plan in
`~/.hermes/plans/codex-workers.md` (Arman's local file).

## Model

- **Backend** is separate from **execution location**. A named session binds
  permanently to one backend at create time. Handle entries carry
  `backend: "codex"`; entries without a `backend` field are Cursor records
  (`handles.backend_of`). Codex IDs (`codex_thread_id`, `codex_turn_id`) are
  persisted next to the human name and never passed to Cursor endpoints.
- **Codex runs locally** in an explicitly selected worktree. No GitHub origin
  or pushed branch is required (that is a Cursor prerequisite only).
- **Independent controller.** One long-lived controller process
  (`codex_controller.py`) owns the `codex app-server` child over stdio, the
  durable per-session state and event log, and the pending completion
  intents. The gateway never spawns app-server itself. The gateway talks
  to the controller over a Unix socket in a 0700 state dir
  (`$XDG_STATE_HOME/ghost_cursor/codex/control.sock`, mode 0600). Nothing
  binds a TCP port.
- **Lifecycle authority** is the controller's observed thread/turn state.
  Gateway death does not touch the turn. On reconnect the gateway follower
  pulls events from the controller from a stable per-session cursor
  (`codex_last_seq` on the handle entry), so a turn that finished while the
  gateway was down still produces a final delivery.
- **Delivery boundary.** The controller persists a completion intent when
  `turn/completed` lands. The gateway enqueues one completion message per
  subscriber onto `process_registry.completion_queue`, then acks the intent.
  Identity is `<session>#codex-turn-<turn_id>` (+ subscriber suffix), so
  follow-up completions are never swallowed by the gateway's
  `(delegation_id, type)` dedupe. Guarantee: at-least-once up to the gateway
  enqueue; a crash between enqueue and ack can duplicate that one message
  (the gateway dedupes by delegation id). Not end-to-end exactly-once.
- **Controller death** is different from gateway death: the client reports
  the last persisted turn as `unknown` (interrupted) and never re-sends the
  prompt. The dispatch intent (`pending_intent`) is persisted before
  `turn/start`, so a retried `codex_send_message` after a controller crash
  is refused when the intent is unresolved rather than silently doubled.
- **Writer exclusion** is one atomic reservation per canonical LOCAL
  worktree (`workers.canonical_repo_path`): `writer_guard.reserve` checks
  and writes the claim file (`claims/<hash>.json`) inside a per-worktree
  `flock` (`claims/<hash>.lock`); the controller uses the same file and lock.
  Cursor local dispatch reserves before `jobs.dispatch` and the job releases
  on observed terminal; Codex reserves in the gateway, the controller
  re-asserts the claim BEFORE `turn/start` and clears it on
  `turn/completed`/boot reconciliation. Liveness: codex claim = controller
  pid alive; cursor claim = gateway pid alive, or the handle still
  `running`, or the worktree's Cursor worker holds a run lease. Cursor cloud
  VMs have no local worktree and are never blocked.
- **Ambiguous dispatch.** A JSON-RPC error answered by the provider on
  `turn/start` is a definitive rejection (intent and claim dropped). A
  transport failure after the request went out (timeout, closed pipe — the
  locally synthesized `code -1` error included) or a success reply without a
  turn id is ambiguous: the intent (`ambiguous: true`) and the claim are
  retained, a later `turn/started` for the thread adopts the turn, and
  further sends are refused. `codex_stop` clears the intent only after the
  controller proves quiescence — its app-server child is dead, or it is the
  only session on that child and the child is torn down (bounded). If other
  sessions are active on the shared child, stop reports `unresolved` and
  keeps intent and claim. No automatic re-send. A steer that races a
  finishing turn returns without starting a new turn.
- **Claim ownership** is per dispatch: the claim carries `owner` (the
  dispatch intent id for Codex; `cursor:<profile>:<session>:<dispatch>` for
  Cursor) and `profile`. `reserve` refuses any live claim with another owner
  — including a second send to the same session — and `release` only removes
  the caller's own owner, so a losing request can never free a winner.
- **Profiles.** The controller is machine-global; handles are per Hermes
  profile. Controller session ids are `<profile-id>:<title>`
  (`codex_client.session_ref`), so equal titles in two profiles never
  collide and a follower only ingests/acks its own profile's sessions.
- **Approvals.** Threads start with `approvalPolicy: "never"` and
  `sandbox: "workspace-write"` (thread/start config mode; `sandboxPolicy.type: workspaceWrite` on turn/start — both verified against codex-cli 0.153.4), explicit and persisted on the session. Any
  server-initiated approval/input request that still arrives is declined
  and recorded as a lifecycle event; the turn fails clearly instead of
  hanging. Interactive approvals are a documented v1 limit.
- **Model/effort** are pinned at create and verified on first dispatch
  against `model/list`. No default or fallback model. Unknown model or
  unsupported effort fails the send with the catalog's exact ids.

## File mapping

| File | Role |
|---|---|
| `codex_protocol.py` | Newline JSON-RPC over stdio (spawn, request/response, notifications, server requests). Stdlib only. Normalizes `item/*`, `turn/*` notifications into the existing envelopes (`content`, `tool_use`, `tool_result`, `file_diff`, `lifecycle`) keeping native ids. |
| `codex_controller.py` | The independent process: socket server, session state, event log, claims, completion intents, model catalog. `python codex_controller.py --state-dir DIR --codex-bin PATH`. |
| `codex_client.py` | Gateway-side client: locate/start the controller (user systemd `systemd-run --user` when available, else detached child reported as degraded), request helpers, follower thread that ingests events into `eventlog`, updates handles, fans out completions. |
| `writer_guard.py` | Cross-backend local worktree writer check used by Cursor local dispatch and Codex send. |
| `__init__.py` | Thin `codex_*` tool entry points and schemas; Cursor local dispatch consults the shared guard. |
| `handles.py` | `backend_of`, codex id fields. |
| `render.py` | Small codex wording variants only where the tool name differs. |
| `test_codex.py` | Integrated fake-protocol workflow (real controller process + fake app-server), gateway client kill/recreate, controller kill, writer-guard primitive. |
| `fixtures/fake_codex_app_server.py` | Scripted app-server speaking the wire protocol for tests. |

## Limits (v1)

- Interactive approvals are declined, not brokered.
- Progress digests and status read files/pending tools/plan from the
  controller's durable per-turn record, so a gateway restart loses no
  projection; completion delivery is the guaranteed path.
- Without user systemd the controller runs detached and status says so.
- Live model acceptance requires an authenticated Codex install; see the
  test log in the PR for what was verified.

## Live protocol evidence (codex-cli 0.153.4, scratch install, no credentials)

Run on the worker host against `/tmp/codex-dev/node_modules/.bin/codex` with an
empty `CODEX_HOME`, through `codex_protocol.AppServer`:

- `initialize` -> `userAgent: hermes_cursor_codex/0.153.4 (...)`, `codexHome`
  honored, `platformFamily: unix`.
- `model/list` (includeHidden) -> 11 models; exact id `gpt-6-astra` present
  (efforts low, medium, high, xhigh, max, ultra; default low).
  `resolve_model(catalog, "gpt-6-astra", None)` resolves without substitution.
- `account/read` -> `{"account": null, "requiresOpenaiAuth": true}`: not
  authenticated. **Live model gate is BLOCKED on auth**, not on protocol.
- `thread/start` with `sandbox: "workspaceWrite"` -> `-32600 unknown variant`;
  with `sandbox: "workspace-write"` -> thread created (`historyMode: paginated`,
  `model: gpt-6-astra`). Controller fixed accordingly.
- `turn/start` accepted (turn id, `status: inProgress`); notifications
  `thread/started`, `thread/status/changed`, `turn/started`, `item/started`
  (userMessage), then `error` notifications with
  `codexErrorInfo.responseStreamDisconnected.httpStatusCode = 401`
  ("Missing bearer or basic authentication").
- `thread/resume` of that thread from a NEW app-server process -> ok;
  `turn/start` on the resumed thread -> ok.
- `turn/steer` with a wrong `expectedTurnId` -> `-32600 expected active turn
  id ... but found ...` (exact mismatch reporting).
- `turn/interrupt` -> `{}` then `turn/completed` with `status: interrupted`.
