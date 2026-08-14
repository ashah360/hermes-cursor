---
name: cursor-agent
description: "Delegate coding to Cursor via the ghost_cursor plugin (named sessions, cloud runtime, seven tools)."
version: 3.0.0
author: Mocha
platforms: [macos]
metadata:
  hermes:
    tags: [cursor, coding, delegation, ghost_cursor]
---

# Cursor delegation (ghost_cursor plugin)

ALL coding work gets delegated to Cursor agents through the plugin's seven tools. Never edit project files directly — create a session, send the task, steer as needed. Even small quick fixes go through cursor.

## Architecture (current, post 2026-07-08)

- Every session is a **cloud agent** (REST v1 + SSE, no local sdk/bridge). `runtime="local"` (default) routes tool-call execution to a plugin-managed "My Machines" worker on this box — work happens in the local checkout; `runtime="cloud"` uses a cursor-hosted VM on a GitHub clone.
- Runs live SERVER-side and survive our process. A **session supervisor** (supervisor.py) owns each run's stream, digests, completion delivery, and settlement; a **reconciler** (plugin init + every 60s) re-attaches supervisors after gateway restarts, adopts legacy handles, and un-settles false failures (remote GET is the settle authority in both directions). Gateway restarts are a non-event for supervision.
- State: handles in `~/.hermes/state/ghost_cursor_handles.json` (name → agent_id, repo, model, subscribers, supervision record); per-session JSONL event log in `~/.hermes/state/ghost_cursor/logs/<session>.jsonl`; worker pidfiles/logs in `~/.hermes/state/ghost_cursor/workers/`.
- Auth: `CURSOR_API_KEY` in `~/.hermes/.env`. Billing = the key owner's cursor account.

## The seven tools

1. **cursor_create_session(repo?, model?, runtime?)** — mints a named handle (`playful-space-bunny`). Dispatches nothing; the agent spawns on first send. Session UUIDs resolve as aliases anywhere a name is accepted.
2. **cursor_send_message(session, message, update_interval_s?, inactivity_timeout_s?, max_wall_s?)** — first message = the task; later messages = follow-ups with full context. If a run is live, a send INTERRUPTS it (native cancel + re-prompt; the ack says so). Auto-subscribes the caller to digests.
3. **cursor_status(session)** — read-only, never disturbs the run. Poll freely.
4. **cursor_events(session, offset=-1, limit=10, kind?)** — pages the JSONL log. Negative offset = from the end. `kind=` filters (reasoning, file_diff, content, tool_use, tool_result, lifecycle).
5. **cursor_stop(session)** — native cancel; acks only after observed termination.
6. **cursor_list(scope='session'|'all')** — TSV of handles. Default scope = sessions this hermes session dispatched.
7. **cursor_subscribe(session, interval_s)** — retune the CALLING session's digest cadence; 0 unsubscribes just this caller. Per-subscriber: every subscribed hermes session gets its own copy.

All outputs are plain text (labeled headers, fenced diffs, TSV). Completion auto-delivers on ANY terminal state — don't poll for it.

## Standard workflow

```
cursor_create_session(repo="/path/to/repo")   → session: brave-lunar-otter
cursor_send_message(session, task)            → runs in background, keep talking
  … digests land as messages; completion auto-delivers
cursor_send_message(session, follow_up)       → same context
```

- One active run per repo path (guard rejects a second). Parallel work → separate worktrees/clones.
- Budget ONE task-sized prompt per session; same-session reuse works but treat it as bonus (see resume rules).
- Repos need git identity for commits (repo-local `git config user.name/email` up front).

## Writing the dispatch brief

- **Spec files referenced in the brief must exist BEFORE dispatch** (write /tmp/spec.md first, then send).
- Structure that works: context/incident → settled design decisions ("do NOT re-derive") → numbered deliverables → blocking test list → binding invariants (no type weakening, no error-swallowing, no `.skip`) → suite invocation (point at the CI workflow file) → push target.
- **WRITE-FIRST, always**: "commit + push a skeleton within 15 minutes, then a commit every logical increment." Cursor's default failure mode is 20-45 min of pure analysis then dying with zero writes. If digests show 10+ min with no file_diff events, send a checkpoint nudge: name its own conclusions back to it (so it doesn't re-derive), order an immediate wip commit+push, then continue.
- For read-only investigations: "Do NOT modify code; the ONLY file you may write is /tmp/<report>.md; cite exact file paths; mark claims verified vs inferred."
- Long shell steps (test suites, builds) emit no stream events → instruct background+poll (`cmd > /tmp/x.log 2>&1 &` + sleep/tail loop) and/or raise `inactivity_timeout_s` (default 600s of SILENCE; activity resets it).

## Models

- On the REST runtime the model string must name a FULL catalog variant: `claude-fable-5[thinking=true,context=300k,effort=medium]`. Bare `-thinking-medium` or partial brackets → `400 invalid_model` (fable variants require thinking+context+effort). Valid combos: `GET /v1/models` (auth w/ CURSOR_API_KEY) → `.items[].variants`.
- Model is fixed per session at create. The API supports per-prompt override (`POST /v1/agents/{id}/runs` accepts `model`) but the plugin doesn't plumb it yet — changing model mid-session needs a fresh session.
- Web research tasks: `model="composer-2.5"`, prompt "WEB RESEARCH only, no code, no file writes, summary as final message text."

## Digests & reporting to the owner

- Digests land in-thread; the owner reads them. Do NOT narrate digests back. Reply only on milestones (first commit, suite result, plan pivot), stalls, or completion — one line, parenthetical.
- Default interval 180s. Retune live w/ cursor_subscribe.
- Digests never fire on runs shorter than the interval — correct behavior, not a bug.
- "files so far" shows UNCOMMITTED work only — after a commit the list empties. Check `git log` in the worktree before claiming work vanished.
- **NEVER report a final result off a digest.** Completion claims require the completion delivery PLUS self-verification: `git log --oneline -1 origin/<branch>` (sha exists remotely) + a content grep. Digest replies carry no shas/counts/CI verdicts.
- Before ACTING on any async message (digest, stale completion receipt), trace every referenced object to the conversation or disk — async messages invite confabulation, and stale receipts from superseded retries keep arriving after you've moved on (recognize by quoted Original goal; reply one line, don't re-act).
- Act as the SENIOR ENGINEER on digests: architecture smells (test concerns leaking into prod code, identifier munging, fix spreading instead of chokepointing) get an immediate corrective interrupt with reasoning inline — not a status relay.

## Failure playbook (current era)

Read the completion notification itself first — `elapsed` + `events since prompt` classify most deaths without extra calls.

- **fast_fail** (<2min, lifecycle-only events, zero progress): transient create/billing class. ONE same-session retry, then a FRESH session with a findings brief. Track the retry count IN the send text ("retry 2/2 — …") — the notification quotes it back, making the chain self-tracking. Two identical instant deaths = wedged, go fresh. On macOS: check the login keychain is unlocked (`security show-keychain-info login.keychain`) before burning retries — a locked keychain mimics this exactly.
- **mid_flight death** (real diffs/commits exist): same-session "continue exactly where you left off" + pointer to the last banked milestone — but only if resumed promptly (<15min idle). Idle >15min → fresh session (expensive-resume rejection class; huge-transcript sessions wedge permanently).
- **Worker not routable**: stale/dead worker records — `pgrep -fl 'agent worker'`, clear `~/.hermes/state/ghost_cursor/workers/*.json` for dead pids, resend (fresh worker spawns). Only one worker per checkout receives assignments.
- **400 "Failed to verify existence of branch"**: cursor's github integration can't see the repo or the branch isn't pushed. Push the branch first; if the repo is missing from `GET /v0/repositories`, the integration lost access — re-grant the repo in cursor's dashboard github settings, or route via a fork the integration CAN see and PR back upstream.
- **409 agent_busy on send**: the run is ALIVE — the plugin now surfaces `still_active` and never settles the handle. Interrupt path (a normal send) cancels first.
- **Bare "status: error"**: true cause is in cursor's store — `for db in $(find ~/.cursor/projects -name index.db); do sqlite3 "$db" "select agent_id,turn_number,status,error_code from runs where status='ERROR' order by started_at desc"; done`. Most common hidden cause per cursor staff: swallowed 429 usage-limit; probe by trying `model auto`.
- **Fresh-session brief after a death**: KEEP vs LOST split — `git status`/`diff` names what survived ("keep it, it's good"), jsonl tail shows what died in-flight ("redo: <spec>"); sections: repo/branch/HEAD → state → root cause → uncommitted-keep → lost-redo → then steps. Harvest the dead run's conclusions from `kind=content` bursts (join `delta` fields) and long tool_results — mark them "hints not gospel".
- **Degenerate reasoning bursts** (repeated/single-char tokens): check cursor_status FIRST — runs self-recover; kill only if activity is stale AND events stay degenerate.
- **Runs surviving OUR failures**: if the plugin loses track but `GET /v0/agents/{id}` says RUNNING, the run is fine — the reconciler adopts/repairs within 60s. Verify remote state before re-dispatching anything.
- **Post-restart digest recovery**: after the gateway/plugin is fixed, `cursor_subscribe` from the same hermes session CAN resume digests MID-RUN (proven live 2026-07-08) — resubscribe and wait one tick before declaring digests dead for that run. While tracking is down, the liveness loop is: send probe → 409 agent_busy = alive, plus `git fetch && git log origin/<branch>` for banked commits.
- **Owner-requested mid-run status (\"interrupt it and ask\")**: send a STATUS CHECK interrupt with a numbered report spec + \"then resume the campaign\". Expect cursor to sometimes skip the prose report and dive straight back into work — the digests carry the story; tell the owner and offer to force it rather than silently re-interrupting.

## Cloud-VM campaign bootstrapping (proven 2026-07-08, twin 300/s loadtest)

`runtime=\"cloud\"` handles heavy docker-stack campaigns, but budget ~30-40min of VM setup before real work and put these in the brief:
- Docker on cursor VMs needs `{\"storage-driver\":\"vfs\"}` (+ containerd-snapshotter false) — nested-overlayfs whiteout failures otherwise. Cursor figures this out but faster if pre-stated.
- The VM's doppler token usually can't read project configs → \"bootstrap synthetic secrets if doppler denies access\".\n- Include an honesty valve: \"if the environment fundamentally can't run the stack, say exactly what's missing and stop — don't fake results.\"
- Loadtest/bench stacks (cloud OR shared box) bind RANDOMIZED ports (probe 20000-60000, retry on collision) + randomized compose project names as a phase-0 HARD requirement — fixed defaults (crdb 26258, temporal 7234, redis 6380) stomp sibling harnesses. Print chosen ports at boot, allow env pinning.
- Restart campaigns from a killed run by harvesting its diagnostic conclusions into the new brief as \"trusted leads, not gospel — verify then fix\".

## Verification rules (non-negotiable)

- Cursor's summary is a self-report. Before PR/claiming done: `git log --oneline -1 origin/<branch>` + rerun the suite locally + grep for the actual change.
- Every send must be followed by the tool ack (`sent to <session> · running in background`) in the same turn — composed-but-not-fired dispatches happen; no ack = it never went out.
- Read final messages from the jsonl, not the truncated completion: `jq -rj 'select(.kind=="content") | .delta // empty' <log>.jsonl`.
- Run the plugin suite the way CI does (copy tree per unit.yml; needs `HERMES_SESSION_*` env ABSENT and a `plugins/__init__.py` in the copied tree). Stash-baseline unexplained failures against clean main before blaming your diff.

## Installing plugin updates

1. Work in a real clone of the plugin repo; branch + push + PR upstream — never leave a hot-deployed install drifted from the repo.
2. Suite green locally (CI-mirror invocation), then sync sources into the live install: `rsync -a --delete --exclude __pycache__ --exclude tests --exclude fixtures --exclude 'test_*' --exclude conftest.py --exclude .git --exclude .github --exclude 'Dockerfile*' --exclude .pytest_cache <clone>/ ~/.hermes/plugins/ghost_cursor/` + py_compile every module with the hermes venv python.
3. Restart the hermes gateway. ORDER MATTERS: sync → verify the new code is in the install dir (grep a new symbol) → restart. A restart before sync is wasted; supervision survives restarts, but the restart still interrupts in-flight hermes turns.

## CI / e2e quirks (upstream repo)

- `unit` = only blocking gate (push-to-main + PRs). `e2e-*` are workflow_dispatch ONLY (each run creates real cloud agents on the account). A pushed branch with no PR gets zero runs.
- Fork PRs never get repo secrets. `gh secret set` works with push access.
- Linux CI health checks that grep ps output MUST use `ps -ww` (80-col truncation false-kills healthy workers).
