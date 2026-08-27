"""Tests for rest_client.py — the SSE parser and the typed REST client.

The parser tests replay the REAL captured streams in ``fixtures/rest_v1/``
(verbatim bytes from api.cursor.com, 2026-07-08) so the wire format is
pinned; the client tests fake the http layer with ``httpx.MockTransport``.
"""

import json
from pathlib import Path

import httpx
import pytest

from plugins.ghost_cursor import rest_client as rc
from plugins.ghost_cursor.rest_client import (
    CursorRestClient,
    RestApiError,
    RestNetworkError,
    SseParser,
    parse_sse_text,
)

FIXTURES = Path(__file__).parent / "fixtures" / "rest_v1"
RUN_A = (FIXTURES / "run_a_tool_heavy.sse").read_bytes()
RUN_B = (FIXTURES / "run_b_edit_heavy.sse").read_bytes()
RUN_C_PRECANCEL = (FIXTURES / "run_c_precancel.sse").read_bytes()
RUN_C_POSTCANCEL = (FIXTURES / "run_c_postcancel.sse").read_bytes()
MODELS_FIXTURE = json.loads((FIXTURES / "models_v1_trimmed.json").read_text())


# ---------------------------------------------------------------------------
# SSE parser — real captured payloads
# ---------------------------------------------------------------------------

class TestSseParserFixtures:
    def test_run_a_event_counts(self):
        """The tool-heavy capture parses to the exact live event census."""
        events = parse_sse_text(RUN_A.decode())
        by_type = {}
        for e in events:
            by_type[e.event] = by_type.get(e.event, 0) + 1
        assert by_type == {
            "status": 3,
            "heartbeat": 2,
            "thinking": 2,
            "assistant": 39,
            "tool_call": 12,
            "interaction_update": 121,
            "result": 1,
            "done": 1,
        }

    def test_all_data_decodes_as_json(self):
        for raw in (RUN_A, RUN_B, RUN_C_PRECANCEL, RUN_C_POSTCANCEL):
            for e in parse_sse_text(raw.decode()):
                assert e.data is not None, f"undecoded data on {e.event}"

    def test_status_events_carry_no_id(self):
        """Per the OpenAPI spec, status events have no id: line (they are
        replayed at the top of every reconnect)."""
        events = parse_sse_text(RUN_A.decode())
        assert all(e.id is None for e in events if e.event == "status")
        assert all(
            e.id is not None for e in events if e.event == "tool_call"
        )

    def test_tool_call_shape(self):
        """tool_call data is the camelCase RunStreamToolCallData shape."""
        calls = [
            e for e in parse_sse_text(RUN_B.decode()) if e.event == "tool_call"
        ]
        assert calls
        for e in calls:
            assert {"callId", "name", "status"} <= set(e.data)
        completed_edit = [
            e for e in calls
            if e.data["name"] == "edit_file" and e.data["status"] == "completed"
        ]
        success = completed_edit[0].data["result"]["success"]
        assert "diffString" in success and "afterFullFileContent" in success

    def test_result_event_of_cancelled_run(self):
        """The cancelled run's replay: simplified status lies (FINISHED),
        the result event carries the true CANCELLED status."""
        events = parse_sse_text(RUN_C_POSTCANCEL.decode())
        statuses = [e.data["status"] for e in events if e.event == "status"]
        assert statuses == ["RUNNING", "FINISHED"]
        result = [e for e in events if e.event == "result"][0]
        assert result.data["status"] == "CANCELLED"

    def test_stream_unavailable_error_event(self):
        """A live stream can emit an error event then done (captured on
        the pre-cancel stream) — it must parse, not raise."""
        events = parse_sse_text(RUN_C_PRECANCEL.decode())
        errors = [e for e in events if e.event == "error"]
        assert errors and errors[0].data["code"] == "stream_unavailable"
        assert events[-1].event == "done"


class TestSseParserIncremental:
    def test_byte_at_a_time_equals_whole(self):
        """Chunk-boundary splits anywhere never change the parse."""
        whole = parse_sse_text(RUN_C_POSTCANCEL.decode())
        parser = SseParser()
        dribbled = []
        for i in range(len(RUN_C_POSTCANCEL)):
            dribbled.extend(parser.feed(RUN_C_POSTCANCEL[i:i + 1]))
        dribbled = [rc.decode_sse_data(e) for e in dribbled]
        assert [(e.event, e.id, e.data) for e in dribbled] == [
            (e.event, e.id, e.data) for e in whole
        ]

    def test_odd_chunk_sizes(self):
        whole = parse_sse_text(RUN_A.decode())
        for size in (7, 64, 1000):
            parser = SseParser()
            events = []
            for i in range(0, len(RUN_A), size):
                events.extend(parser.feed(RUN_A[i:i + size]))
            events.extend(parser.feed("\n\n"))
            assert len(events) == len(whole), f"chunk size {size}"

    def test_crlf_and_cr_line_endings(self):
        doc = b'event: status\r\ndata: {"a": 1}\r\n\r\n'
        events = SseParser().feed(doc)
        assert len(events) == 1 and events[0].raw_data == '{"a": 1}'
        # CRLF split across the chunk boundary
        parser = SseParser()
        events = parser.feed(b'event: x\r\ndata: 1\r')
        events += parser.feed(b'\n\r\n')
        assert len(events) == 1 and events[0].event == "x"

    def test_multi_line_data_joins_with_newline(self):
        events = SseParser().feed("data: line1\ndata: line2\n\n")
        assert events[0].raw_data == "line1\nline2"

    def test_comment_lines_ignored(self):
        events = SseParser().feed(": keepalive\ndata: 1\n\n")
        assert len(events) == 1 and events[0].raw_data == "1"

    def test_blank_line_without_data_dispatches_nothing(self):
        assert SseParser().feed("event: ghost\n\n") == []

    def test_malformed_line_logged_and_skipped(self, caplog):
        with caplog.at_level("WARNING", logger="plugins.ghost_cursor.rest_client"):
            events = SseParser().feed("bogusfield: nope\ndata: 1\n\n")
        assert len(events) == 1  # the event still dispatched
        assert any("malformed" in r.message for r in caplog.records)

    def test_undecodable_data_yields_raw(self, caplog):
        with caplog.at_level("WARNING", logger="plugins.ghost_cursor.rest_client"):
            events = parse_sse_text("event: assistant\ndata: {not json\n\n")
        assert len(events) == 1
        assert events[0].data is None
        assert events[0].raw_data == "{not json"
        assert any("undecodable" in r.message for r in caplog.records)

    def test_last_event_id_tracked(self):
        parser = SseParser()
        parser.feed("id: 100-0\nevent: a\ndata: 1\n\nevent: b\ndata: 2\n\n")
        # The id-less second event does NOT reset the tracker.
        assert parser.last_event_id == "100-0"

    def test_id_with_nul_ignored(self):
        events = SseParser().feed("id: a\x00b\ndata: 1\n\n")
        assert events[0].id is None

    def test_eof_flush_dispatches_final_event(self):
        """A document ending without a trailing blank line still yields
        its final event via parse_sse_text's EOF flush."""
        events = parse_sse_text('event: done\ndata: {}')
        assert len(events) == 1 and events[0].event == "done"


# ---------------------------------------------------------------------------
# REST client — faked transport
# ---------------------------------------------------------------------------

def _client_with(handler):
    return CursorRestClient(
        api_key="key-test", transport=httpx.MockTransport(handler)
    )


class TestRestClientRequests:
    def test_create_agent_body_and_response(self):
        seen = {}

        def handler(request):
            seen["auth"] = request.headers.get("authorization")
            seen["body"] = json.loads(request.content)
            seen["path"] = request.url.path
            return httpx.Response(201, json={
                "agent": {"id": "bc-1", "url": "https://cursor.com/agents/bc-1"},
                "run": {"id": "run-1", "agentId": "bc-1", "status": "CREATING"},
            })

        with _client_with(handler) as client:
            out = client.create_agent(
                "do things",
                model_id="claude-fable-5",
                env={"type": "machine", "name": "w1"},
                repos=[{"url": "https://github.com/o/r", "startingRef": "main"}],
                mcp_servers=[{
                    "name": "paper",
                    "type": "stdio",
                    "command": "/usr/bin/python",
                    "args": ["/plugin/proxy.py", "http://127.0.0.1:29979/mcp"],
                }],
                work_on_current_branch=True,
                name="my session",
            )
        assert seen["auth"] == "Bearer key-test"
        assert seen["path"] == "/v1/agents"
        assert seen["body"] == {
            "prompt": {"text": "do things"},
            "model": {"id": "claude-fable-5"},
            "env": {"type": "machine", "name": "w1"},
            "repos": [{"url": "https://github.com/o/r", "startingRef": "main"}],
            "mcpServers": [{
                "name": "paper",
                "type": "stdio",
                "command": "/usr/bin/python",
                "args": ["/plugin/proxy.py", "http://127.0.0.1:29979/mcp"],
            }],
            "workOnCurrentBranch": True,
            "name": "my session",
        }
        assert out["agent"]["id"] == "bc-1" and out["run"]["id"] == "run-1"

    def test_create_agent_omits_absent_fields(self):
        seen = {}

        def handler(request):
            seen["body"] = json.loads(request.content)
            return httpx.Response(201, json={"agent": {}, "run": {}})

        with _client_with(handler) as client:
            client.create_agent("task only", mcp_servers=[])
        assert seen["body"] == {"prompt": {"text": "task only"}}

    def test_followup_posts_runs(self):
        seen = {}

        def handler(request):
            seen["path"] = request.url.path
            seen["body"] = json.loads(request.content)
            return httpx.Response(201, json={"run": {"id": "run-2"}})

        with _client_with(handler) as client:
            out = client.send_followup("bc-1", "more work")
        assert seen["path"] == "/v1/agents/bc-1/runs"
        assert seen["body"] == {"prompt": {"text": "more work"}}
        assert out["run"]["id"] == "run-2"

    def test_cancel_conflict_is_typed(self):
        def handler(request):
            return httpx.Response(409, json={"error": {
                "code": "run_not_cancellable",
                "message": "Run is already finished",
            }})

        with _client_with(handler) as client:
            with pytest.raises(RestApiError) as err:
                client.cancel_run("bc-1", "run-1")
        assert err.value.status_code == 409
        assert err.value.code == "run_not_cancellable"
        assert not err.value.retryable

    def test_list_models_parses_fixture(self):
        def handler(request):
            assert request.url.path == "/v1/models"
            return httpx.Response(200, json=MODELS_FIXTURE)

        with _client_with(handler) as client:
            models = client.list_models()
        ids = [m["id"] for m in models]
        assert "composer-2.5" in ids and len(models) == 4

    def test_empty_api_key_rejected(self):
        with pytest.raises(rc.RestClientError):
            CursorRestClient(api_key="  ")


class TestRestClientRetries:
    @pytest.fixture(autouse=True)
    def _no_backoff(self, monkeypatch):
        monkeypatch.setattr(rc, "_GET_RETRY_BACKOFF_S", (0.0, 0.0))

    def test_get_retries_5xx_then_succeeds(self):
        calls = []

        def handler(request):
            calls.append(1)
            if len(calls) < 3:
                return httpx.Response(503, json={"error": {"code": "unavailable"}})
            return httpx.Response(200, json={"id": "bc-1", "status": "ACTIVE"})

        with _client_with(handler) as client:
            out = client.get_agent("bc-1")
        assert len(calls) == 3 and out["status"] == "ACTIVE"

    def test_get_retries_exhaust_and_raise(self):
        def handler(request):
            return httpx.Response(500, json={"error": {"code": "boom"}})

        with _client_with(handler) as client:
            with pytest.raises(RestApiError) as err:
                client.list_agents()
        assert err.value.status_code == 500

    def test_get_does_not_retry_4xx(self):
        calls = []

        def handler(request):
            calls.append(1)
            return httpx.Response(404, json={"error": {
                "code": "agent_not_found", "message": "nope",
            }})

        with _client_with(handler) as client:
            with pytest.raises(RestApiError) as err:
                client.get_agent("bc-x")
        assert len(calls) == 1 and err.value.code == "agent_not_found"

    def test_get_retries_network_errors(self):
        calls = []

        def handler(request):
            calls.append(1)
            if len(calls) == 1:
                raise httpx.ConnectError("refused")
            return httpx.Response(200, json={"items": []})

        with _client_with(handler) as client:
            out = client.list_runs("bc-1")
        assert len(calls) == 2 and out == {"items": []}

    def test_post_is_never_retried(self):
        """A create whose response was lost may still have happened —
        blind-retrying POSTs would double-create agents."""
        calls = []

        def handler(request):
            calls.append(1)
            return httpx.Response(503, json={"error": {"code": "unavailable"}})

        with _client_with(handler) as client:
            with pytest.raises(RestApiError):
                client.create_agent("task")
        assert len(calls) == 1

    def test_post_network_error_is_typed_not_retried(self):
        calls = []

        def handler(request):
            calls.append(1)
            raise httpx.ReadTimeout("slow")

        with _client_with(handler) as client:
            with pytest.raises(RestNetworkError):
                client.send_followup("bc-1", "task")
        assert len(calls) == 1


class TestRestClientStream:
    def test_stream_yields_decoded_events(self):
        def handler(request):
            assert request.headers.get("accept") == "text/event-stream"
            return httpx.Response(
                200,
                content=RUN_C_POSTCANCEL,
                headers={"content-type": "text/event-stream"},
            )

        with _client_with(handler) as client:
            events = list(client.stream_run_events("bc-1", "run-1"))
        assert [e.event for e in events] == [
            "status", "status", "result", "done",
        ]
        assert events[2].data["status"] == "CANCELLED"

    def test_stream_sends_last_event_id_header(self):
        seen = {}

        def handler(request):
            seen["header"] = request.headers.get("last-event-id")
            return httpx.Response(200, content=b"event: done\ndata: {}\n\n")

        with _client_with(handler) as client:
            list(client.stream_run_events("bc-1", "run-1", last_event_id="42-0"))
        assert seen["header"] == "42-0"

    def test_stream_http_error_is_typed(self):
        def handler(request):
            return httpx.Response(410, json={"error": {
                "code": "stream_expired", "message": "gone",
            }})

        with _client_with(handler) as client:
            with pytest.raises(RestApiError) as err:
                list(client.stream_run_events("bc-1", "run-1"))
        assert err.value.status_code == 410 and err.value.code == "stream_expired"

    def test_stream_drop_mid_body_is_network_error(self):
        def handler(request):
            def broken():
                yield b"event: status\ndata: {}\n\n"
                raise httpx.ReadError("connection lost")

            return httpx.Response(200, content=broken())

        with _client_with(handler) as client:
            received = []
            with pytest.raises(RestNetworkError):
                for event in client.stream_run_events("bc-1", "run-1"):
                    received.append(event)
        # Events before the drop were delivered — the caller reconnects
        # with the last seen id.
        assert [e.event for e in received] == ["status"]
