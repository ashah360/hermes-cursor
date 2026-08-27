# Hermes Cursor

[![CI](https://github.com/ashah360/hermes-cursor/actions/workflows/unit.yml/badge.svg)](https://github.com/ashah360/hermes-cursor/actions/workflows/unit.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Hermes Cursor lets [Hermes Agent](https://github.com/NousResearch/hermes-agent) delegate coding work to Cursor Cloud Agents and keep the job inside the conversation where it started.

Ask Hermes to fix a bug, build a feature, or review a repository. Hermes starts a named Cursor session in the background, sends progress back to chat, and delivers the result when the run finishes. You can run the work on a Cursor-hosted VM or route it to your own machine through Cursor's My Machines runtime.

- Keep talking to Hermes while Cursor works.
- Watch progress without disturbing the run.
- Continue the same Cursor session with follow-up instructions.
- Open the session in Cursor's Agents UI at any time.
- Run separate Git worktrees in parallel on your own machine.

## Quick start

You need:

- Hermes Agent with plugin support.
- A paid Cursor plan and a personal Cursor API key.
- The Cursor GitHub app authorized for the repository you want to use.

### Install

```bash
hermes plugins install ashah360/hermes-cursor --enable
```

Configuration uses the plugin key `ghost_cursor`.

### Add your Cursor API key

Create a key in the [Cursor dashboard](https://cursor.com/dashboard), then find the Hermes environment file:

```bash
hermes config env-path
```

Add the key to that file:

```dotenv
CURSOR_API_KEY=your_cursor_api_key
```

The Cursor account behind the key must have access to the target repository through its GitHub connection.

Restart the Hermes process that will use the plugin. For the messaging gateway:

```bash
hermes gateway restart
```

For the CLI, exit and start a new Hermes session.

### Optional: enable local execution

Install the Cursor Agent CLI on the machine running Hermes:

```bash
curl -fsSL https://cursor.com/install | bash
```

You only need this for `runtime="local"`. Hosted runs do not require the local Cursor CLI. Your Cursor account must also have access to My Machines.

## Use it

Talk to Hermes normally:

> Use Cursor on a hosted VM to fix the flaky webhook tests in this repository. Add a regression test and report what changed.

Or send work to your own machine:

> Use Cursor Cloud on my local machine for this task. Create a separate worktree, run the backend test suite, and keep me updated every minute.

Hermes creates the Cursor session, sends the task, monitors it, and posts the final result back into the conversation.

## Choose where the work runs

| | `runtime="cloud"` | `runtime="local"` |
|---|---|---|
| Execution | Cursor-hosted VM | The machine running Hermes |
| Repository | GitHub clone | Existing local checkout or worktree |
| Best for | Isolated changes and ordinary coding tasks | Large builds, local services, warm caches, and private test infrastructure |
| Cursor Agent CLI on the Hermes machine | Not required | Required |
| Visible in Cursor's Agents UI | Yes | Yes |

Local mode is still a Cursor Cloud Agent. Cursor owns the agent conversation; its terminal and file operations are routed to a managed worker on your machine. The worker can run commands and edit files in the selected checkout with your user permissions.

Hermes Cursor isolates each local worktree in its own worker runtime. Separate worktrees can run concurrently without sharing Cursor's global worker lock. A local dispatch fails clearly if its selected worker cannot be reached; it never falls back silently to a hosted VM. Active work is protected from cleanup, idle workers are retired automatically, and workers survive Hermes gateway restarts when user systemd is available. If systemd is unavailable, status output clearly marks the worker as using degraded detached supervision.

## Sessions and follow-ups

A named session is a continuing conversation with one Cursor agent:

```text
Fix payment webhook retries
├── reproduce the failure and implement the fix
├── add the missing regression test
└── address review feedback
```

Each follow-up keeps the existing Cursor context and produces its own completion message.

Sending a follow-up while a run is still active interrupts that run; it does not queue behind it. Changes already written to the working tree remain, but the in-flight step is cancelled and replaced with the new instruction. Wait for the current run to finish if you do not want to interrupt it.

## Progress and control

Hermes automatically subscribes the calling conversation to progress updates when it sends a task. The final result is delivered separately on every outcome: completed, failed, cancelled, or timed out.

The plugin provides seven tools:

| Tool | What it does |
|---|---|
| `cursor_create_session` | Create a named session and choose the repository, model, and runtime. |
| `cursor_send_message` | Start work or send a contextual follow-up. |
| `cursor_status` | Read the current state without affecting the run. |
| `cursor_events` | Inspect reasoning, tool activity, content, and file changes. |
| `cursor_stop` | Cancel the active run through Cursor. |
| `cursor_list` | List recorded Cursor sessions. |
| `cursor_subscribe` | Change progress-update frequency for the current Hermes conversation. |

Hermes normally chooses and calls these tools for you.

## Repository requirements

Cursor must be able to access the repository through GitHub.

For local execution, use a Git checkout with a GitHub `origin` and a named branch. Push a newly-created branch before the first run so Cursor can validate it:

```bash
git push -u origin HEAD
```

For hosted execution, Hermes Cursor accepts either a GitHub-backed local checkout or a `github.com` repository URL.

## Configuration

All settings are optional:

```yaml
plugins:
  ghost_cursor:
    model: null                 # use Cursor's default model
    inactivity_timeout_s: 600  # stop after this much stream silence; 0 disables
    max_wall_s: 0              # total run limit; 0 disables
    max_workers: 10            # local worker capacity
    worker_idle_ttl_s: 1800    # local worker idle lifetime in seconds
    local_mcp_servers:          # optional, runtime="local" only
      paper:
        url: http://127.0.0.1:29979/mcp
```

A tool argument overrides the configured default for that run.

`local_mcp_servers` exposes host-local Streamable HTTP MCP endpoints to new
local-worker sessions through a bundled stdio proxy. Each mapping key becomes
the MCP server name. Servers are attached only when a fresh
`runtime="local"` agent is created; hosted agents never receive them, and
follow-ups retain the original agent configuration. This is an explicit
host-local integration point, not Paper-specific behavior. This initial
configuration supports only an unauthenticated `http` or `https` URL (no
headers or secrets).

Progress updates default to every 180 seconds. Ask Hermes for a different interval, pass `update_interval_s` with the task, or use `cursor_subscribe`. Setting the interval to `0` stops progress updates for that Hermes conversation but does not disable the final result.

## What survives a restart

Cursor runs independently of Hermes. If the gateway restarts, Hermes Cursor reconnects to active runs and resumes terminal delivery. In local mode, systemd-supervised workers also survive the restart and are adopted by the new gateway process.

## Troubleshooting

### The tools do not appear

```bash
hermes plugins list
```

Confirm that `ghost_cursor` is installed and enabled, then restart the gateway or start a new CLI session.

### Cursor reports a missing branch

Push the current branch to GitHub:

```bash
git push -u origin HEAD
```

### The local worker asks for authentication

Make sure `CURSOR_API_KEY` is in the file reported by `hermes config env-path`, then restart Hermes.

### Local worker capacity is full

Wait for a local run to finish, stop one with `cursor_stop`, or raise `plugins.ghost_cursor.max_workers` if your Cursor plan and machine support more workers. Hermes Cursor never evicts a worker that still has protected work.

### A follow-up cancelled the current run

That is the expected interrupt behavior. Wait for a run to finish before sending the next instruction if you want sequential execution.

## Update

```bash
hermes plugins update ghost_cursor
```

Restart the gateway or CLI process after updating.

## License

[MIT](LICENSE)
