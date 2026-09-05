"""Plain-text renderings for every ghost_cursor tool result (v0.4).

Target formats: ``/tmp/gc-v04-formats.md`` (schematic T-050, approved
2026-07-02). Construction is pure python f-string templating — no LLM calls
anywhere in this module (the aux-model peek line is deliberately absent; see
the TODO at the ``working on:`` line).

House rules (from the spec):

* no ``success`` booleans — the ``status:`` line carries the state;
* labeled header lines, then plain english, then raw fenced ```` ```diff ````
  blocks — a diff is NEVER JSON-escaped;
* reasoning/thinking content NEVER appears in status output; peek/summary
  lines clip hard; full text only via ``cursor_events`` (and even there each
  event's inline text clips at 2 KB with a pointer to the JSONL log);
* every clip carries an explicit marker + where to find the full content;
* TSV (header row, aligned) for ``cursor_list`` rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import eventlog as _eventlog

# ---------------------------------------------------------------------------
# Bounds (all from the format spec)
# ---------------------------------------------------------------------------

# 'working on:' line — first 100 chars of the task string.
WORKING_ON_CHARS = 100
# status peek lines (summary of a finished run shown by cursor_status).
STATUS_PEEK_CHARS = 200
# cursor's final summary in a completion message.
SUMMARY_CHARS = 2000
# 'recent' bullets: last ~5 event titles, each clipped to 120 chars.
RECENT_BULLETS = 5
BULLET_CHARS = 120
# completion diff bounding: full diffs only when <=5 files AND <=200 total
# diff lines; otherwise counts+paths for all + diffs for the largest 3.
FULL_DIFF_MAX_FILES = 5
FULL_DIFF_MAX_LINES = 200
LARGEST_DIFF_COUNT = 3
# cursor_events: per-event inline content clip + total response cap.
EVENT_INLINE_CLIP = 2048
EVENTS_RESPONSE_CAP = 20 * 1024
EVENTS_TRUNCATION_NOTE = "page truncated at 20KB — narrow with limit/kind"
# progress digest: events-since-last-tick body caps (~5 lines / ~1KB).
DIGEST_MAX_EVENTS = 5
DIGEST_BODY_CAP = 1024
# digest/status 'waiting on' block: max in-flight calls listed.
PENDING_MAX_ROWS = 4

_STATUS_WORDS = {"A": "added", "M": "modified", "D": "deleted"}


def _plan_line(plan: Any, where: str = "cursor_events") -> str:
    """'plan: 2/5 done — current: <item>' from cursor's todo snapshot, or
    '' when no plan was streamed / the shape is unusable."""
    if not isinstance(plan, list) or not plan:
        return ""
    items = [p for p in plan if isinstance(p, dict) and p.get("content")]
    if not items:
        return ""
    done = sum(1 for p in items if "complet" in str(p.get("status") or ""))
    current = next(
        (
            str(p.get("content"))
            for p in items
            if "progress" in str(p.get("status") or "")
        ),
        "",
    )
    line = f"plan: {done}/{len(items)} done"
    if current:
        line += f" — current: {clip(current, 120, where)}"
    return line


# ---------------------------------------------------------------------------
# Small shared pieces
# ---------------------------------------------------------------------------

def secs(seconds: Any) -> str:
    """'43s' — whole seconds, template-friendly."""
    try:
        return f"{int(float(seconds))}s"
    except (TypeError, ValueError):
        return "—"


def dur_compact(seconds: Any) -> str:
    """'45s' / '3m' / '2m30s' / '4h' — compact duration for header lines.

    "" when the value is absent, unparseable, or <= 0 (callers omit the
    fragment entirely). Sub-second values round up to '1s' so a live
    subscription never renders as zero.
    """
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return ""
    if value <= 0:
        return ""
    total = max(int(round(value)), 1)
    if total < 60:
        return f"{total}s"
    minutes, rem_s = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{rem_s}s" if rem_s else f"{minutes}m"
    hours, rem_m = divmod(minutes, 60)
    return f"{hours}h{rem_m}m" if rem_m else f"{hours}h"


def age_compact(seconds: Any) -> str:
    """'45s' / '3m' / '4h' / '2d' — coarse age for prose ('' when unknown)."""
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return ""
    if value <= 0:
        return ""
    total = max(int(round(value)), 1)
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m"
    if total < 86400:
        return f"{total // 3600}h"
    return f"{total // 86400}d"


def clip(text: Any, limit: int, where: str = "cursor_events") -> str:
    """Hard clip with an explicit marker + where the full content lives."""
    s = str(text or "")
    if len(s) <= limit:
        return s
    return s[:limit] + f"… [+{len(s) - limit} chars — full text via {where}]"


def _clip_kb(text: str, limit: int = EVENT_INLINE_CLIP) -> str:
    """The 2 KB per-event inline clip, marker per the format spec."""
    if len(text) <= limit:
        return text
    clipped_k = max(1, round((len(text) - limit) / 1024))
    return text[:limit] + (
        f"… [clipped {clipped_k}k — full event in the JSONL log]"
    )


def _one_line(text: Any) -> str:
    return " ".join(str(text or "").split())


def since_prompt_line(name: str, total_events: Any, last_prompt_seq: Any) -> str:
    """'events since prompt: N — cursor_events(...)' — the labeled line
    shared by status / events / completion output.

    N is how many event-log items accumulated since the session was last
    prompted, and the pointer names the exact offset/limit that pages
    them. Sessions with no recorded marker (fresh, or legacy handles from
    before the field existed) count the whole log.
    """
    total = max(int(total_events or 0), 0)
    marker = max(int(last_prompt_seq or 0), 0)
    since = max(total - marker, 0)
    if since == 0:
        return "events since prompt: 0"
    limit = min(since, _eventlog.MAX_PAGE_LIMIT)
    return (
        f"events since prompt: {since} — "
        f"cursor_events('{name}', offset={marker}, limit={limit})"
    )


def files_inline(files: List[Dict[str, Any]], max_files: int = 8) -> str:
    """'calc.py +4 −0, test.py +12 −0' (counts only, never diffs)."""
    parts = [
        f"{f.get('path', '?')} +{f.get('added', 0)} −{f.get('removed', 0)}"
        for f in files[:max_files]
    ]
    if len(files) > max_files:
        parts.append(f"… {len(files) - max_files} more")
    return ", ".join(parts)


def _file_rows(files: List[Dict[str, Any]]) -> str:
    """One row per file: 'calc.py  +4 −0  modified'."""
    return "\n".join(
        f"{f.get('path', '?')}  +{f.get('added', 0)} −{f.get('removed', 0)}  "
        f"{_STATUS_WORDS.get(str(f.get('status') or 'M'), str(f.get('status') or 'modified'))}"
        for f in files
    )


def _diff_fence(diff: str) -> str:
    """A raw fenced diff block. NEVER JSON-escape a diff."""
    body = diff.rstrip("\n")
    return f"```diff\n{body}\n```"


# ---------------------------------------------------------------------------
# cursor_create_session / cursor_send_message
# ---------------------------------------------------------------------------

def create_session_ack(
    name: str, repo: str, model: Optional[str], runtime: str = "local"
) -> str:
    return (
        f"session: {name}\n"
        f"repo: {repo} · model: {model or 'default'} · runtime: {runtime}\n"
        "created. send work with cursor_send_message."
    )


def title_too_long(title: str, limit: int) -> str:
    """The over-long-title create failure: name the cap and ask for a
    shorter phrase — never silently truncate (a truncated title could
    collide, and it reads badly on cursor.com)."""
    return (
        f"cannot create session: the title is {len(title)} chars — the max "
        f"is {limit}. shorten it to a concise phrase of roughly 3-8 words, "
        "like a short commit subject (e.g. 'Fix payment webhook retries')."
    )


def title_taken(title: str, status: str, age_s: Any = None) -> str:
    """The duplicate-title create failure: name the existing entry's state
    ('completed, 2d ago' vs 'running') and ask for a more specific title —
    never auto-suffix silently."""
    age = age_compact(age_s)
    state = f"{status}, {age} ago" if age else status
    return (
        f"cannot create session: the title '{title}' is already in use by "
        f"an existing session ({state}). pick a different, more specific "
        "title for this task — e.g. name the component, bug, or feature "
        "it covers."
    )


def send_ack(name: str, interrupted: bool = False) -> str:
    lines = [
        f"sent to {name} · running in background",
        "result auto-delivers; cursor_status polls; a second send "
        "interrupts + re-prompts.",
    ]
    if interrupted:
        lines.append(
            "note: interrupted mid-run — in-flight step discarded, "
            "continuing with context."
        )
    return "\n".join(lines)


def repo_busy(active_session: str, repo: str) -> str:
    active = active_session or "(handle pending)"
    return (
        f"cannot start: session '{active}' is already running in {repo} and "
        "two cursor runs on one working tree corrupt it. send the work to "
        f"that session with cursor_send_message('{active}', ...), watch it "
        f"with cursor_status('{active}'), or stop it first with "
        f"cursor_stop('{active}')."
    )


# ---------------------------------------------------------------------------
# cursor_status
# ---------------------------------------------------------------------------

def recent_bullets(events: List[Dict[str, Any]]) -> List[str]:
    """Template-rendered bullets from the last event titles, newest last.

    Reasoning and raw content chunks are excluded entirely (status output
    never carries thinking text); bullets clip at 120 chars.
    """
    bullets: List[str] = []
    for record in events:
        kind = _eventlog.display_kind(record)
        if kind == "tool_use":
            tool = str(record.get("tool") or "")
            if tool == "shell":
                # Description-first title when the normalizer salvaged one;
                # raw command as fallback.
                detail = _one_line(record.get("title") or record.get("command"))
                bullets.append(f"ran `{detail}`" if detail else "ran a command")
            elif tool == "subagent":
                title = _one_line(record.get("title") or "sub-agent")
                bullets.append(f'spawned subagent "{title}"')
            else:
                title = _one_line(record.get("title") or record.get("path"))
                bullets.append(title or "tool call")
        elif kind == "file_diff":
            bullets.append(
                f"edited {record.get('path', '?')} "
                f"+{record.get('added', 0)} −{record.get('removed', 0)}"
            )
    return [clip(b, BULLET_CHARS) for b in bullets[-RECENT_BULLETS:]]


def status_text(
    *,
    name: str,
    status: str,
    elapsed_s: Any,
    last_activity_s: Any,
    total_events: int,
    log_path: Optional[str],
    task: str,
    files: List[Dict[str, Any]],
    bullets: List[str],
    summary: str = "",
    error: str = "",
    note: str = "",
    pending_tools: Optional[List[Dict[str, Any]]] = None,
    plan: Optional[List[Dict[str, str]]] = None,
    last_prompt_seq: Any = 0,
    runtime: str = "",
    worker: str = "",
    agents_ui_url: str = "",
) -> str:
    """The read-only status view. NO inline diffs, NO reasoning content."""
    log = log_path or "(none yet)"
    last = f"{secs(last_activity_s)} ago" if last_activity_s is not None else "—"
    runtime_bits = [f"runtime: {runtime}"] if runtime else []
    if worker:
        runtime_bits.append(f"worker: {worker}")
    if agents_ui_url:
        runtime_bits.append(agents_ui_url)
    lines = [
        f"status: {status}",
        f"session: {name} · elapsed: {secs(elapsed_s)} · "
        f"last activity: {last}",
        *([" · ".join(runtime_bits)] if runtime_bits else []),
        f"events: {total_events} total · log: {log}",
        since_prompt_line(name, total_events, last_prompt_seq),
        "",
        # TODO(peek design, T-050 §aux-model): replace this template-only
        # line with the aux-model one-line peek once that lands. Until then
        # it is the task string verbatim — never LLM-generated.
        f"working on: {clip(_one_line(task), WORKING_ON_CHARS)}",
    ]
    plan_line = _plan_line(plan)
    if plan_line:
        lines.append(plan_line)
    if pending_tools:
        lines.append("waiting on:")
        for p in pending_tools[:PENDING_MAX_ROWS]:
            since = p.get("pending_s")
            suffix = f" ({secs(since)})" if since is not None else ""
            lines.append(
                f"  {clip(_one_line(p.get('title') or 'tool'), 160)}{suffix}"
            )
        if len(pending_tools) > PENDING_MAX_ROWS:
            lines.append(
                f"  … {len(pending_tools) - PENDING_MAX_ROWS} more in-flight calls"
            )
    if summary:
        lines += ["", f"summary: {clip(_one_line(summary), STATUS_PEEK_CHARS)}"]
    if error:
        lines += ["", f"error: {_one_line(error)}"]
    if files:
        lines += [
            "",
            f"files so far ({len(files)}): {files_inline(files)}",
            f"diffs: cursor_events('{name}', kind='file_diff')",
        ]
    if bullets:
        lines += ["", "recent:"] + [f"- {b}" for b in bullets]
    if note:
        lines += ["", note]
    return "\n".join(lines)


def subscribe_ack(name: str, interval_s: float, note: Optional[str] = None) -> str:
    """The 1-line cursor_subscribe ack — the subscription belongs to the
    CALLING hermes session. ``note`` is the validation clamp sentence
    (progress.validate_interval), appended in-line when present."""
    if interval_s <= 0:
        return (
            f"session '{name}': progress updates off for this hermes "
            "session (other subscribers and completion delivery "
            "unaffected)."
        )
    text = (
        f"session '{name}': progress updates every {secs(interval_s)} to "
        "this hermes session (applies to the running/next run; takes "
        "effect on the next tick)."
    )
    return f"{text} {note}" if note else text


# ---------------------------------------------------------------------------
# progress digest (periodic subscription updates)
# ---------------------------------------------------------------------------

def digest_text(
    *,
    name: str,
    n: int,
    status: str,
    elapsed_s: Any,
    last_activity_s: Any,
    files: List[Dict[str, Any]],
    pending_tool: str = "",
    pending_tool_s: Any = None,
    pending_tools: Optional[List[Dict[str, Any]]] = None,
    plan: Optional[List[Dict[str, str]]] = None,
    events: Optional[List[Dict[str, Any]]] = None,
    new_count: int = 0,
    next_update_s: Any = None,
    backend: str = "cursor",
) -> str:
    """One periodic progress digest: the cursor_status-style header plus
    the events since the previous tick (cursor_events-style lines, capped
    at DIGEST_MAX_EVENTS lines / DIGEST_BODY_CAP chars). Tagged with the
    session name and the digest number so concurrent sessions stay
    distinguishable. ``next_update_s`` is the ticker's CURRENT interval at
    tick time — a mid-run cursor_subscribe change shows in the very next
    digest — rendered as 'next update in 3m' on the status line (omitted
    when unknown/<= 0).

    ``pending_tools`` (list of {tool, title, pending_s}) supersedes the
    scalar ``pending_tool``/``pending_tool_s`` pair, which stays accepted
    for callers without the richer snapshot. ``plan`` is cursor's own
    todo list ({content, status} dicts) when one was streamed.

    ``backend`` names the agent and its events tool; Cursor is the default.
    """
    events_tool = f"{backend}_events"
    last = f"{secs(last_activity_s)} ago" if last_activity_s is not None else "—"
    next_update = dur_compact(next_update_s)
    lines = [
        f"{backend} session '{name}' — progress update {n}",
        f"status: {status} · elapsed: {secs(elapsed_s)} · last activity: {last}"
        + (f" · next update in {next_update}" if next_update else ""),
    ]
    plan_line = _plan_line(plan, events_tool)
    if plan_line:
        lines.append(plan_line)
    if files:
        lines.append(f"files so far ({len(files)}): {files_inline(files)}")
    pending = list(pending_tools or [])
    if not pending and pending_tool:
        pending = [{"title": pending_tool, "pending_s": pending_tool_s}]
    if pending:
        lines.append("waiting on:")
        for p in pending[:PENDING_MAX_ROWS]:
            since = p.get("pending_s")
            suffix = f" ({secs(since)})" if since is not None else ""
            lines.append(f"  {clip(_one_line(p.get('title') or 'tool'), 160, events_tool)}{suffix}")
        if len(pending) > PENDING_MAX_ROWS:
            lines.append(f"  … {len(pending) - PENDING_MAX_ROWS} more in-flight calls")
    lines.append("")
    if not events:
        if pending:
            lines.append(
                f"no new stream events — {backend} is busy inside the calls above"
            )
        else:
            lines.append("no new events since last update")
        return "\n".join(lines)

    lines.append(f"new events since last update ({new_count}):")
    used = 0
    rows = 0
    for record in events[-DIGEST_MAX_EVENTS:]:
        row = clip(
            f"{record.get('seq')}  {_eventlog.display_kind(record):<11}  "
            f"{_event_summary(record, events_tool)}",
            200, events_tool,
        )
        if used + len(row) + 1 > DIGEST_BODY_CAP:
            break
        lines.append(row)
        used += len(row) + 1
        rows += 1
    if new_count > rows:
        lines.append(f"… {new_count - rows} more — {events_tool}('{name}')")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# completion message (all terminal states) + cursor_stop
# ---------------------------------------------------------------------------

def _bounded_diff_blocks(name: str, files: List[Dict[str, Any]]) -> List[str]:
    """Fenced diff blocks per the bounding rule (see module docstring)."""
    with_diffs = [f for f in files if str(f.get("diff") or "").strip()]
    total_lines = sum(
        len(str(f.get("diff") or "").splitlines()) for f in with_diffs
    )
    if len(files) <= FULL_DIFF_MAX_FILES and total_lines <= FULL_DIFF_MAX_LINES:
        return [_diff_fence(str(f["diff"])) for f in with_diffs]

    largest = sorted(
        with_diffs,
        key=lambda f: len(str(f.get("diff") or "").splitlines()),
        reverse=True,
    )[:LARGEST_DIFF_COUNT]
    blocks = [
        f"largest {len(largest)} diffs of {len(files)} files:"
    ] if largest else []
    blocks += [_diff_fence(str(f["diff"])) for f in largest]
    blocks.append(
        f"remaining diffs: cursor_events('{name}', kind='file_diff') "
        "or the event log above."
    )
    return blocks


def _retry_qualifier(retryable: Any, retry_after: Any) -> str:
    """' (retryable, retry after 30s)' from typed error detail; '' when
    nothing is known (None = unknown, never rendered as 'not retryable')."""
    parts = []
    if retryable is True:
        parts.append("retryable")
    elif retryable is False:
        parts.append("not retryable")
    if retry_after:
        after = str(retry_after)
        try:
            after = secs(float(after))
        except (TypeError, ValueError):
            pass  # HTTP-date form — render verbatim
        parts.append(f"retry after {after}")
    return f" ({', '.join(parts)})" if parts else ""


def completion_text(
    *,
    name: str,
    status: str,
    elapsed_s: Any,
    repo: str,
    summary: str,
    files: List[Dict[str, Any]],
    error: str = "",
    total_events: Any = 0,
    last_prompt_seq: Any = 0,
    retryable: Any = None,
    retry_after: Any = None,
) -> str:
    """The terminal-state report (delivered message / in-turn fast finish).

    ``retryable`` / ``retry_after`` are the typed error fields mined from a
    terminal-error run (see cloud_runner) — rendered as a parenthetical on
    the failure line: "run failed: ServerError: … (retryable, retry after
    30s)".
    """
    lines = [
        f"status: {status}",
        f"session: {name} · elapsed: {secs(elapsed_s)} · repo: {repo}",
        since_prompt_line(name, total_events, last_prompt_seq),
    ]
    if error:
        lines += [
            "",
            f"run {status}: {_one_line(error)}"
            f"{_retry_qualifier(retryable, retry_after)}. working-tree "
            "changes are intact. send another message to the same session "
            "to continue — transient failures usually succeed on retry.",
        ]
    if summary:
        clipped = summary[:SUMMARY_CHARS]
        note = (
            f"\n… [+{len(summary) - SUMMARY_CHARS} chars — full summary via "
            f"cursor_events('{name}', kind='content')]"
            if len(summary) > SUMMARY_CHARS
            else ""
        )
        lines += ["", "cursor's summary:", clipped + note]
    if files:
        lines += ["", f"files changed ({len(files)}):", _file_rows(files)]
        blocks = _bounded_diff_blocks(name, files)
        if blocks:
            lines += [""] + blocks
    elif not error:
        lines += ["", "no files were changed."]
    return "\n".join(lines)


def stop_text(
    *,
    name: str,
    status: str,
    elapsed_s: Any,
    files: List[Dict[str, Any]],
    already_finished: bool = False,
) -> str:
    if already_finished:
        second = (
            f"session: {name} · already finished ({secs(elapsed_s)} run) — "
            "nothing to stop"
        )
    else:
        second = (
            f"session: {name} · stopped after {secs(elapsed_s)} "
            "(native cancel)"
        )
    if files:
        work = (
            f"partial work: {len(files)} "
            f"file{'s' if len(files) != 1 else ''} — {files_inline(files)} "
            "(diffs in event log)"
        )
    else:
        work = "partial work: none"
    return "\n".join([
        f"status: {status}",
        second,
        work,
        "the session stays continuable — cursor_send_message picks the "
        "work back up with full context.",
    ])


# ---------------------------------------------------------------------------
# cursor_events
# ---------------------------------------------------------------------------

def _event_summary(record: Dict[str, Any], where: str = "cursor_events") -> str:
    kind = _eventlog.display_kind(record)
    if kind == "tool_use":
        tool = str(record.get("tool") or "tool")
        # The normalizer's title is description-first ("Start focused rush
        # install in tmux background"); raw command stays the fallback.
        detail = _one_line(record.get("title") or record.get("command") or "")
        if tool == "subagent":
            model = str(record.get("subagent_model") or "")
            suffix = f" [{model}]" if model else ""
            return f'subagent "{detail}"{suffix}' if detail else f"subagent{suffix}"
        return f"{tool} `{detail}`" if detail else tool
    if kind == "tool_result":
        status = str(record.get("status") or "done")
        dur = record.get("durationMs")
        suffix = f" ({dur / 1000:.1f}s)" if isinstance(dur, (int, float)) else ""
        head = _one_line(record.get("output") or "")
        head = f" {head}" if head else ""
        return clip(f"→ {status}{head}{suffix}", 160, where)
    if kind == "file_diff":
        return (
            f"{record.get('path', '?')} +{record.get('added', 0)} "
            f"−{record.get('removed', 0)}"
        )
    if kind == "reasoning":
        return clip(f'"{_one_line(record.get("text"))}"', 160, where)
    if kind == "content":
        return clip(f'"{_one_line(record.get("delta"))}"', 160, where)
    if kind == "lifecycle":
        event = str(record.get("event") or "lifecycle")
        err = _one_line(record.get("error") or "")
        return clip(f"{event}: {err}" if err else event, 160, where)
    return kind


def _event_body(record: Dict[str, Any]) -> str:
    """Full event content (clipped at 2 KB), below the summary line.

    Diffs render as raw fenced blocks; long text/output as plain text. Short
    single-line content already fits the summary line and renders nothing.
    """
    kind = _eventlog.display_kind(record)
    if kind == "file_diff":
        diff = str(record.get("diff") or "")
        return _diff_fence(_clip_kb(diff)) if diff.strip() else ""
    field = {
        "reasoning": "text",
        "content": "delta",
        "tool_result": "output",
    }.get(kind)
    if not field:
        return ""
    text = str(record.get(field) or "")
    if len(text) <= 100 and "\n" not in text:
        return ""  # already fully visible in the summary line
    return _clip_kb(text)


def events_text(name: str, page: Dict[str, Any], last_prompt_seq: Any = 0) -> str:
    events = page.get("events") or []
    total = int(page.get("total_events") or 0)
    log = str(page.get("log_path") or "")
    kind = page.get("kind")

    if not events:
        scope = f" of kind '{kind}'" if kind else ""
        return (
            f"no events{scope} in the requested window for session "
            f"'{name}' ({total} events total · log: {log})."
        )

    first, last = events[0].get("seq"), events[-1].get("seq")
    matching = int(page.get("total_matching") or total)
    of = (
        f"{matching} matching (kind={kind}) of {total}" if kind else f"{total}"
    )
    chunks = [
        f"events {first}–{last} of {of} (log: {log})",
        since_prompt_line(name, total, last_prompt_seq),
    ]

    for gap in page.get("gaps") or []:
        gfirst, glast = gap.get("first_dropped_seq"), gap.get("last_dropped_seq")
        chunks.append(f"gap: seqs {gfirst}–{glast} compacted away")

    body_chunks: List[str] = []
    for record in events:
        seq = record.get("seq")
        line = f"{seq}  {_eventlog.display_kind(record):<11}  {_event_summary(record)}"
        body = _event_body(record)
        body_chunks.append(f"{line}\n{body}" if body else line)

    # Total response cap: drop whole trailing events once past ~20 KB.
    out: List[str] = list(chunks)
    used = sum(len(c) + 1 for c in out)
    truncated = False
    for chunk in body_chunks:
        if used + len(chunk) + 1 > EVENTS_RESPONSE_CAP:
            truncated = True
            break
        out.append(chunk)
        used += len(chunk) + 1
    if truncated:
        out.append(EVENTS_TRUNCATION_NOTE)
    return "\n".join(out)


def no_event_log(name: str) -> str:
    return (
        f"no events recorded for session '{name}' yet — the log appears "
        "with the first cursor_send_message activity."
    )


# ---------------------------------------------------------------------------
# cursor_list + unknown-session errors
# ---------------------------------------------------------------------------

_LIST_COLUMNS = (
    "session", "repo", "runtime", "status", "elapsed", "files", "last_activity"
)


def list_text(rows: List[Dict[str, str]]) -> str:
    """TSV with a header row; fields space-padded so columns align."""
    table = [dict(zip(_LIST_COLUMNS, _LIST_COLUMNS))] + [
        {c: str(r.get(c, "—")) for c in _LIST_COLUMNS} for r in rows
    ]
    widths = {
        c: max(len(row[c]) for row in table) for c in _LIST_COLUMNS
    }
    return "\n".join(
        "\t".join(row[c].ljust(widths[c]) for c in _LIST_COLUMNS).rstrip()
        for row in table
    )


def empty_list(scope: str) -> str:
    if scope == "all":
        return "no cursor sessions exist yet. create one with cursor_create_session."
    return (
        "no cursor sessions in this hermes session. create one with "
        "cursor_create_session (scope='all' shows every session)."
    )


def unknown_session(identifier: str, rows: List[Dict[str, str]]) -> str:
    if rows:
        return (
            f"no session named '{identifier}'. sessions in this hermes "
            f"session:\n{list_text(rows)}\n(scope='all' shows every session)"
        )
    return (
        f"no session named '{identifier}', and this hermes session has no "
        "sessions yet. create one with cursor_create_session, or pass "
        "scope='all' to cursor_list to see every session."
    )
