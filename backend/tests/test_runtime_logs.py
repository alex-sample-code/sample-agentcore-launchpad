"""GET /api/agents/{id}/runtime-logs — an agent's Runtime stdout/stderr.

Hermetic: the logs client is a stub serving scripted DescribeLogStreams /
FilterLogEvents pages and recording every call. The properties asserted: the
log group comes from the ledger row (never the caller), a session reads the
streams named after it (or, on a Harness, events naming it), ``otel-rt-logs``
is excluded by default, a missing group / denied read degrades to
``unavailable_reason``, and hidden agents / private sessions answer 404.
"""

import json
from typing import Any

import pytest

import app.routers.agents as agents_router
from app.core.db import DEFAULT_WORKSPACE_ID, SessionLocal
from app.models.ledger import Agent
from app.services import runtime_logs
from app.services.workspace import WorkspaceContext
from tests.conftest import client_error

RUNTIME_ID = "hr_assistant-AbCdEf1234"
GROUP = f"/aws/bedrock-agentcore/runtimes/{RUNTIME_ID}-DEFAULT"
SID = "sess-0123456789abcdef0123456789abcdef"
OTHER = "sess-ffffffffffffffffffffffffffffffff"
NOW = 1_800_000_000_000


def _stream(name: str, last: int = NOW - 1000) -> dict:
    return {"logStreamName": name, "lastEventTimestamp": last, "firstEventTimestamp": last - 10}


def _otel(sid: str) -> str:
    return json.dumps({"attributes": {"session.id": sid}})


def _event(stream: str, message: str, ts: int = NOW - 5000) -> dict:
    return {"logStreamName": stream, "message": message, "timestamp": ts}


SESSION_STREAM = f"2026/10/19/[runtime-logs-{SID}]aaaa"
OTHER_STREAM = f"2026/10/19/[runtime-logs-{OTHER}]bbbb"


class StubLogs:
    def __init__(self, streams=None, pages=None, groups=None, error=None):
        self.streams = streams or []
        self.pages = list(pages or [{"events": []}])
        self.groups = groups or []
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    def describe_log_streams(self, **kw):
        self.calls.append(("describe_log_streams", kw))
        if self.error:
            raise self.error
        return {"logStreams": self.streams}

    def filter_log_events(self, **kw):
        self.calls.append(("filter_log_events", kw))
        if self.error:
            raise self.error
        return self.pages.pop(0) if len(self.pages) > 1 else self.pages[0]

    def describe_log_groups(self, **kw):
        self.calls.append(("describe_log_groups", kw))
        return {"logGroups": self.groups}

    def filters(self) -> list[dict]:
        return [kw for op, kw in self.calls if op == "filter_log_events"]


@pytest.fixture
def logs(monkeypatch):
    def install(**kw: Any) -> StubLogs:
        stub = StubLogs(**kw)
        monkeypatch.setattr(WorkspaceContext, "client", lambda self, name, **k: stub)
        return stub

    monkeypatch.setattr(runtime_logs, "_now_ms", lambda: NOW)
    return install


def _seed(method: str = "zip_runtime", resource_id: str | None = RUNTIME_ID,
          status: str = "active", spec: dict | None = None) -> str:
    db = SessionLocal()
    try:
        row = Agent(workspace_id=DEFAULT_WORKSPACE_ID, name=f"a-{method}", method=method,
                    status=status,
                    resource_id=resource_id, spec=spec or {})
        db.add(row)
        db.commit()
        return row.id
    finally:
        db.close()


def test_without_session_reads_the_derived_group_and_drops_otel(client, logs):
    stub = logs(
        streams=[_stream(SESSION_STREAM), _stream("otel-rt-logs")],
        pages=[{"events": [
            _event(SESSION_STREAM, "Starting agent"),
            _event("otel-rt-logs", _otel(SID)),
            _event(SESSION_STREAM, "Traceback (most recent call last)"),
        ], "nextToken": "aws-1"}],
    )
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs?log_group=/evil&limit=2")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["log_group"] == GROUP
    assert "log-group/$252Faws$252Fbedrock-agentcore$252Fruntimes" in body["cloudwatch_url"]
    assert [e["message"] for e in body["events"]] == [
        "Starting agent", "Traceback (most recent call last)",
    ]
    assert body["events"][0]["session_id"] == SID
    assert body["sessions"] == [SID]
    assert body["unavailable_reason"] is None
    (first, *_) = stub.filters()
    assert first["logGroupName"] == GROUP
    assert first["startTime"] == NOW - 3600 * 1000 and first["endTime"] == NOW
    assert "logStreamNames" not in first and "filterPattern" not in first

    # the page token pins the first page's window and carries the AWS token
    res = client.get(f"/api/agents/{agent_id}/runtime-logs",
                     params={"range": "7d", "next_token": body["next_token"]})
    again = stub.filters()[-1]
    assert again["nextToken"] == "aws-1"
    assert again["startTime"] == NOW - 3600 * 1000


def test_session_reads_its_own_streams_and_keyword_is_a_quoted_term(client, logs):
    stub = logs(
        streams=[_stream(SESSION_STREAM), _stream(OTHER_STREAM), _stream("otel-rt-logs")],
        pages=[{"events": [_event(SESSION_STREAM, "tool failed: timeout")]}],
    )
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs",
                     params={"session_id": SID, "keyword": "tool failed", "range": "24h"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["session_match"] == "stream"
    assert body["next_token"] is None
    call = stub.filters()[0]
    assert call["logStreamNames"] == [SESSION_STREAM]
    assert call["filterPattern"] == '"tool failed"'
    assert call["startTime"] == NOW - 24 * 3600 * 1000


def test_session_with_otel_keeps_only_that_sessions_otel_events(client, logs):
    stub = logs(
        streams=[_stream(SESSION_STREAM), _stream("otel-rt-logs")],
        pages=[{"events": [
            _event(SESSION_STREAM, "hello"),
            _event("otel-rt-logs", _otel(SID)),
            _event("otel-rt-logs", _otel(OTHER)),
        ]}],
    )
    agent_id = _seed()
    body = client.get(f"/api/agents/{agent_id}/runtime-logs",
                      params={"session_id": SID, "include_otel": "true"}).json()
    assert stub.filters()[0]["logStreamNames"] == [SESSION_STREAM, "otel-rt-logs"]
    assert [e["stream"] for e in body["events"]] == [SESSION_STREAM, "otel-rt-logs"]


def test_harness_discovers_its_backing_group_and_matches_sessions_by_content(client, logs):
    backing = "/aws/bedrock-agentcore/runtimes/harness_hr_harness-Zz9-DEFAULT"
    stub = logs(
        groups=[
            {"logGroupName": "/aws/bedrock-agentcore/runtimes/harness_hr_harness-Old-DEFAULT",
             "creationTime": 1},
            {"logGroupName": backing, "creationTime": 2},
        ],
        streams=[_stream("[runtime-logs]1111-2222"), _stream("otel-rt-logs")],
        pages=[{"events": [_event("[runtime-logs]1111-2222", f"handling {SID}")]}],
    )
    agent_id = _seed(method="harness", resource_id="hr_harness-XyZ987")
    body = client.get(f"/api/agents/{agent_id}/runtime-logs",
                      params={"session_id": SID}).json()
    assert body["log_group"] == backing
    assert body["session_match"] == "content"
    assert body["events"][0]["session_id"] is None
    call = stub.filters()[0]
    assert call["logGroupName"] == backing
    assert call["filterPattern"] == f'"{SID}"' and "logStreamNames" not in call
    describe = [kw for op, kw in stub.calls if op == "describe_log_groups"][0]
    assert describe["logGroupNamePrefix"] == (
        "/aws/bedrock-agentcore/runtimes/harness_hr_harness-"
    )


def test_harness_before_its_first_session_has_no_group(client, logs):
    logs(groups=[])
    agent_id = _seed(method="harness", resource_id="hr_harness-XyZ987")
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert body["unavailable_reason"] == "log_group_not_found"
    assert body["events"] == []


@pytest.mark.parametrize(("code", "reason"), [
    ("ResourceNotFoundException", "log_group_not_found"),
    ("AccessDeniedException", "access_denied"),
])
def test_missing_group_or_denied_read_degrades(client, logs, code, reason):
    logs(error=client_error(code, operation="FilterLogEvents"))
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs")
    assert res.status_code == 200, res.text
    assert res.json()["unavailable_reason"] == reason
    assert res.json()["log_group"] == GROUP  # the deep link still helps


@pytest.mark.parametrize("kw", [
    {"resource_id": None},
    {"status": "deleted"},
    {"method": "unknown_method"},
])
def test_agent_without_a_runtime_reads_nothing(client, logs, kw):
    stub = logs()
    agent_id = _seed(**kw)
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert body["unavailable_reason"] == "no_runtime"
    assert stub.calls == []


def test_missing_agent_is_404(client, logs):
    res = client.get("/api/agents/nope/runtime-logs")
    assert res.status_code == 404
    assert res.json()["code"] == "agent.not_found"


@pytest.mark.parametrize("params", [
    {"session_id": "short"},
    {"session_id": 'sess-12345678"|x'},
    {"range": "30d"},
    {"limit": 201},
])
def test_invalid_input_is_422(client, logs, params):
    stub = logs()
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs", params=params)
    assert res.status_code == 422, res.text
    assert stub.calls == []


def test_forged_next_token_is_422(client, logs):
    logs()
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs", params={"next_token": "!!"})
    assert res.status_code == 422
    assert res.json()["code"] == "runtime_logs.bad_token"


class _Private:
    def __init__(self, *_a):
        pass

    def visible(self, session_id):
        return session_id != OTHER

    def require_visible(self, session_id):
        if not self.visible(session_id):
            from app.core.errors import NotFoundError
            raise NotFoundError("observability.session_not_found", "session not found")


def test_private_sessions_are_404_and_filtered_out(client, logs, monkeypatch):
    monkeypatch.setattr(agents_router, "PrivateSessions", _Private)
    stub = logs(
        streams=[_stream(SESSION_STREAM), _stream(OTHER_STREAM)],
        pages=[{"events": [_event(SESSION_STREAM, "mine"), _event(OTHER_STREAM, "theirs")]}],
    )
    agent_id = _seed()
    res = client.get(f"/api/agents/{agent_id}/runtime-logs", params={"session_id": OTHER})
    assert res.status_code == 404
    assert stub.calls == []
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["message"] for e in body["events"]] == ["mine"]
    assert body["sessions"] == [SID]
