"""Typed REST v1 client for Cursor cloud agents — the plugin's ONLY http layer.

Replaces the python ``cursor-sdk`` (and its ``cursor-sdk-bridge`` sidecar)
entirely: every call here is a direct https request to ``api.cursor.com``
authenticated with ``Bearer CURSOR_API_KEY``. No bridge, no sidecar
process, no local state. The wire contract is covered by sanitized captures
in ``fixtures/rest_v1/`` and the published Cursor Cloud Agents OpenAPI spec.

Endpoints wrapped (all verified live):

* ``POST /v1/agents``                       — create agent + initial run
* ``POST /v1/agents/{id}/runs``             — follow-up prompt (new run)
* ``GET  /v1/agents/{id}``                  — agent detail
* ``GET  /v1/agents?limit=N``               — list agents
* ``GET  /v1/agents/{id}/runs``             — list runs
* ``GET  /v1/agents/{id}/runs/{runId}``     — run status (settle authority)
* ``POST /v1/agents/{id}/runs/{runId}/cancel`` — cancel (terminal)
* ``GET  /v1/agents/{id}/runs/{runId}/stream`` — SSE event stream
* ``GET  /v1/models``                       — model catalog (ids + aliases)
* ``GET  /v1/me``                           — cheap auth check

Error model: every failure raises a typed :class:`RestClientError` —
:class:`RestApiError` for non-2xx responses (carries ``status_code`` and
the server's ``error.code`` / ``error.message``), :class:`RestNetworkError`
for connect/read/protocol failures. Nothing is swallowed.

Retry policy: bounded backoff retries on 429/5xx and network errors for
IDEMPOTENT GETs only. POSTs are NEVER blind-retried — a create/followup
whose response was lost may still have happened server-side (live
observation: ``POST /v1/agents`` can take >30s; the agent exists even if
the caller timed out).

SSE: :class:`SseParser` is an incremental byte-fed parser implementing the
relevant subset of the WHATWG EventSource spec (chunk-boundary splits,
CR/LF/CRLF line endings, comment lines, multi-line ``data:``, ``id:``
tracking). Malformed lines are logged with their raw content and skipped —
never raised, never silently dropped. ``stream_run_events`` layers JSON
decoding on top: an event whose data fails to parse is still yielded with
``data=None`` and the raw text preserved so the consumer can passthrough.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.cursor.com"
API_KEY_ENV = "CURSOR_API_KEY"

# Timeouts (seconds). Create/followup POSTs get a long read timeout: the
# live probe saw POST /v1/agents exceed 30s before returning 201.
CONNECT_TIMEOUT_S = 15.0
READ_TIMEOUT_S = 60.0
POST_READ_TIMEOUT_S = 180.0
# Gap tolerance between SSE chunks. The server emits heartbeat events, so
# a healthy stream never goes this quiet; a read timeout means the stream
# is dead and the caller should reconnect with Last-Event-ID.
STREAM_READ_TIMEOUT_S = 300.0

# Bounded retries for idempotent GETs on 429/5xx/network errors.
MAX_GET_ATTEMPTS = 3
# Backoff ladder between GET retry attempts (module-level so tests can
# zero it). A parseable Retry-After header overrides the step.
_GET_RETRY_BACKOFF_S = (1.0, 4.0)

_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class RestClientError(Exception):
    """Base typed failure for every cursor REST call."""


class RestApiError(RestClientError):
    """A non-2xx response, carrying the server's error code and message."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str = "",
        retry_after: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = str(code or "")
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        return self.status_code in _RETRYABLE_STATUS


class RestNetworkError(RestClientError):
    """Connect/read/protocol failure — the response never (fully) arrived."""


# ---------------------------------------------------------------------------
# SSE parsing
# ---------------------------------------------------------------------------

@dataclass
class SseEvent:
    """One parsed server-sent event.

    ``data`` is the JSON-decoded payload (``stream_run_events`` decodes it);
    ``raw_data`` always keeps the verbatim text so a payload that fails to
    decode is never lost. ``id`` is None for events the server sends without
    an ``id:`` line (the replayed ``status`` events, per the OpenAPI spec).
    """

    event: str
    data: Any
    id: Optional[str] = None
    raw_data: str = ""


class SseParser:
    """Incremental SSE parser: feed raw bytes, collect complete events.

    Implements the WHATWG EventSource dispatch rules this API exercises:

    * events are dispatched on a blank line, and only when the data buffer
      is non-empty;
    * multiple ``data:`` lines join with ``\\n``;
    * a single leading space after the field colon is stripped;
    * lines starting with ``:`` are comments (ignored);
    * ``id:`` sets the event id (ids containing NUL are ignored per spec);
    * CR, LF, and CRLF line endings all terminate a line, including split
      across chunk boundaries.

    A line that is neither blank, a comment, nor a known field is logged
    with its raw content and skipped — the stream keeps parsing.
    """

    _FIELDS = ("event", "data", "id", "retry")

    def __init__(self) -> None:
        self._buffer = ""
        # True when the previous chunk ended in CR: a LF at the start of
        # the next chunk belongs to that CRLF pair, not a new blank line.
        self._pending_cr = False
        self._event_type = ""
        self._data_lines: List[str] = []
        self._event_id: Optional[str] = None
        self.last_event_id: Optional[str] = None

    def feed(self, chunk: bytes | str) -> List[SseEvent]:
        """Consume one chunk, returning every event it completed."""
        text = chunk.decode("utf-8", "replace") if isinstance(chunk, bytes) else chunk
        if self._pending_cr:
            if text.startswith("\n"):
                text = text[1:]
            self._pending_cr = False
        self._buffer += text
        if self._buffer.endswith("\r"):
            self._buffer = self._buffer[:-1]
            self._pending_cr = True
            trailing_cr = True
        else:
            trailing_cr = False

        lines = self._buffer.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        # The final element is an incomplete line (or "" after a newline) —
        # keep it buffered for the next chunk. A trailing lone CR completed
        # its line, so nothing carries over in that case.
        if trailing_cr:
            self._buffer = ""
            complete = lines
        else:
            self._buffer = lines.pop()
            complete = lines

        events: List[SseEvent] = []
        for line in complete:
            event = self._line(line)
            if event is not None:
                events.append(event)
        return events

    def _line(self, line: str) -> Optional[SseEvent]:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None  # comment
        field, sep, value = line.partition(":")
        if not sep:
            field, value = line, ""
        if value.startswith(" "):
            value = value[1:]
        if field not in self._FIELDS:
            logger.warning("SSE: skipping malformed line %r", line[:500])
            return None
        if field == "event":
            self._event_type = value
        elif field == "data":
            self._data_lines.append(value)
        elif field == "id":
            if "\x00" not in value:
                self._event_id = value
        # "retry" is accepted (valid SSE) but unused: reconnect pacing is
        # the consumer's bounded-backoff loop.
        return None

    def _dispatch(self) -> Optional[SseEvent]:
        data_lines, event_type, event_id = (
            self._data_lines, self._event_type, self._event_id
        )
        self._data_lines, self._event_type, self._event_id = [], "", None
        if not data_lines:
            return None
        if event_id is not None:
            self.last_event_id = event_id
        return SseEvent(
            event=event_type or "message",
            data=None,
            id=event_id,
            raw_data="\n".join(data_lines),
        )


def decode_sse_data(event: SseEvent) -> SseEvent:
    """The event with ``data`` JSON-decoded from ``raw_data``.

    A payload that fails to decode is logged with the raw line and the
    event still flows (``data=None``) — the consumer maps it to a generic
    envelope instead of dropping it.
    """
    try:
        return SseEvent(
            event=event.event,
            data=json.loads(event.raw_data),
            id=event.id,
            raw_data=event.raw_data,
        )
    except ValueError:
        logger.warning(
            "SSE: undecodable data for event %r: %r",
            event.event, event.raw_data[:500],
        )
        return event


def parse_sse_text(text: str) -> List[SseEvent]:
    """Every decoded event in a complete SSE document (fixture replay)."""
    parser = SseParser()
    events = [decode_sse_data(e) for e in parser.feed(text)]
    # A final event not followed by a blank line still dispatches at EOF.
    tail = parser.feed("\n\n")
    events.extend(decode_sse_data(e) for e in tail)
    return events


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CursorRestClient:
    """Small typed client owning ALL http traffic to the cursor REST API.

    ``transport`` is a test seam (``httpx.MockTransport``); production use
    passes only ``api_key``.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        if not str(api_key or "").strip():
            raise RestClientError(f"{API_KEY_ENV} is empty — no API key to send")
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(
                CONNECT_TIMEOUT_S,
                read=READ_TIMEOUT_S,
                write=READ_TIMEOUT_S,
                pool=CONNECT_TIMEOUT_S,
            ),
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CursorRestClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # -- plumbing ------------------------------------------------------------

    @staticmethod
    def _error_from_response(response: httpx.Response) -> RestApiError:
        code, message = "", ""
        try:
            detail = response.json().get("error")
            if isinstance(detail, dict):
                code = str(detail.get("code") or "")
                message = str(detail.get("message") or "")
        except ValueError:
            pass
        text = message or response.text[:300] or response.reason_phrase
        return RestApiError(
            f"cursor api {response.request.method} "
            f"{response.request.url.path} -> {response.status_code}"
            f"{f' {code}' if code else ''}: {text}",
            status_code=response.status_code,
            code=code,
            retry_after=response.headers.get("retry-after"),
        )

    @staticmethod
    def _retry_delay_s(exc: Exception, attempt: int) -> float:
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            try:
                return max(float(str(retry_after)), 0.0)
            except (TypeError, ValueError):
                pass  # HTTP-date form — fall through to the ladder
        return _GET_RETRY_BACKOFF_S[
            min(attempt, len(_GET_RETRY_BACKOFF_S) - 1)
        ]

    def _get_json(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """GET with bounded 429/5xx/network retries (idempotent only)."""
        last_exc: Optional[Exception] = None
        for attempt in range(MAX_GET_ATTEMPTS):
            try:
                response = self._client.get(path, params=params)
                if response.status_code >= 400:
                    error = self._error_from_response(response)
                    if not error.retryable or attempt == MAX_GET_ATTEMPTS - 1:
                        raise error
                    last_exc = error
                else:
                    return response.json()
            except httpx.HTTPError as exc:
                if attempt == MAX_GET_ATTEMPTS - 1:
                    raise RestNetworkError(
                        f"cursor api GET {path} failed: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                last_exc = exc
            delay = self._retry_delay_s(last_exc, attempt)
            logger.warning(
                "cursor api GET %s attempt %d failed (%s) — retrying in %.1fs",
                path, attempt + 1, last_exc, delay,
            )
            time.sleep(delay)
        raise RestClientError(f"cursor api GET {path}: retry loop exhausted")

    def _post_json(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST, single attempt — creates/cancels are never blind-retried."""
        try:
            response = self._client.post(
                path,
                json=body,
                timeout=httpx.Timeout(
                    CONNECT_TIMEOUT_S,
                    read=POST_READ_TIMEOUT_S,
                    write=READ_TIMEOUT_S,
                    pool=CONNECT_TIMEOUT_S,
                ),
            )
        except httpx.HTTPError as exc:
            raise RestNetworkError(
                f"cursor api POST {path} failed: {type(exc).__name__}: {exc}"
            ) from exc
        if response.status_code >= 400:
            raise self._error_from_response(response)
        return response.json()

    # -- agents ----------------------------------------------------------------

    def create_agent(
        self,
        prompt_text: str,
        *,
        model_id: Optional[str] = None,
        model_params: Optional[List[Dict[str, str]]] = None,
        env: Optional[Dict[str, str]] = None,
        repos: Optional[List[Dict[str, str]]] = None,
        work_on_current_branch: Optional[bool] = None,
        name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """``POST /v1/agents`` → ``{"agent": {...}, "run": {...}}``.

        ``env`` is the AgentEnv dict (``{"type": "machine", "name": ...}``
        for a self-hosted worker; omit for a cursor-hosted VM). ``repos``
        entries are RepoConfig dicts (``{"url", "startingRef"}``).
        """
        body: Dict[str, Any] = {
            "prompt": {"text": str(prompt_text)},
            **(
                {
                    "model": {
                        "id": model_id,
                        **({"params": model_params} if model_params else {}),
                    }
                }
                if model_id
                else {}
            ),
            **({"env": env} if env else {}),
            **({"repos": repos} if repos is not None else {}),
            **(
                {"workOnCurrentBranch": bool(work_on_current_branch)}
                if work_on_current_branch is not None
                else {}
            ),
            **({"name": name} if name else {}),
        }
        return self._post_json("/v1/agents", body)

    def send_followup(self, agent_id: str, prompt_text: str) -> Dict[str, Any]:
        """``POST /v1/agents/{id}/runs`` → ``{"run": {...}}``.

        409 (another run active on the agent) raises RestApiError with the
        server's code — the caller decides whether to cancel-and-retry.
        """
        return self._post_json(
            f"/v1/agents/{agent_id}/runs", {"prompt": {"text": str(prompt_text)}}
        )

    def get_agent(self, agent_id: str) -> Dict[str, Any]:
        return self._get_json(f"/v1/agents/{agent_id}")

    def list_agents(self, limit: int = 20) -> Dict[str, Any]:
        return self._get_json("/v1/agents", params={"limit": int(limit)})

    # -- runs ------------------------------------------------------------------

    def list_runs(self, agent_id: str, limit: int = 20) -> Dict[str, Any]:
        return self._get_json(
            f"/v1/agents/{agent_id}/runs", params={"limit": int(limit)}
        )

    def get_run(self, agent_id: str, run_id: str) -> Dict[str, Any]:
        """``GET /v1/agents/{id}/runs/{runId}`` — the terminal-status
        authority (the simplified SSE ``status`` events are not; a
        cancelled run replays ``status: FINISHED`` — captured live,
        fixtures/rest_v1/run_c_postcancel.sse)."""
        return self._get_json(f"/v1/agents/{agent_id}/runs/{run_id}")

    def cancel_run(self, agent_id: str, run_id: str) -> Dict[str, Any]:
        """``POST .../cancel``. Terminal; a run already settled raises
        RestApiError(code="run_not_cancellable", status_code=409)."""
        return self._post_json(f"/v1/agents/{agent_id}/runs/{run_id}/cancel", {})

    # -- models / auth -----------------------------------------------------------

    def list_models(self) -> List[Dict[str, Any]]:
        """``GET /v1/models`` items — each carries id, displayName,
        aliases, parameters, variants."""
        items = self._get_json("/v1/models").get("items")
        return items if isinstance(items, list) else []

    def me(self) -> Dict[str, Any]:
        return self._get_json("/v1/me")

    # -- SSE stream -----------------------------------------------------------

    def stream_run_events(
        self,
        agent_id: str,
        run_id: str,
        last_event_id: Optional[str] = None,
    ) -> Iterator[SseEvent]:
        """Yield decoded SSE events for one run until the stream closes.

        Pass ``last_event_id`` on reconnect to resume after a drop (the
        server replays the id-less ``status`` events at the top of every
        reconnect, then continues after the given id). Server-side
        retention is ~4 days; past it the endpoint returns 410
        (RestApiError). Mid-stream connection failures raise
        RestNetworkError — the caller reconnects with the last seen id.
        """
        headers = {"Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = str(last_event_id)
        parser = SseParser()
        try:
            with self._client.stream(
                "GET",
                f"/v1/agents/{agent_id}/runs/{run_id}/stream",
                headers=headers,
                timeout=httpx.Timeout(
                    CONNECT_TIMEOUT_S,
                    read=STREAM_READ_TIMEOUT_S,
                    write=READ_TIMEOUT_S,
                    pool=CONNECT_TIMEOUT_S,
                ),
            ) as response:
                if response.status_code >= 400:
                    response.read()
                    raise self._error_from_response(response)
                for chunk in response.iter_bytes():
                    for event in parser.feed(chunk):
                        yield decode_sse_data(event)
                for event in parser.feed("\n\n"):  # EOF flush
                    yield decode_sse_data(event)
        except httpx.HTTPError as exc:
            raise RestNetworkError(
                f"cursor api stream for {run_id} dropped: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
