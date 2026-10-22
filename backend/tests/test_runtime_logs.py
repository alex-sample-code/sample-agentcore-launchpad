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
from app.models.assistant import AssistantConversation
from app.models.ledger import Agent
from app.services import runtime_logs
from app.services.workspace import WorkspaceContext
from tests.conftest import client_error

RUNTIME_ID = "hr_assistant-AbCdEf1234"
GROUP = f"/aws/bedrock-agentcore/runtimes/{RUNTIME_ID}-DEFAULT"
SID = "sess-0123456789abcdef0123456789abcdef"
OTHER = "sess-ffffffffffffffffffffffffffffffff"
NOW = 1_800_000_000_000
# the architect preset: the only agent its assistant conversations' private sessions run on
PRESET = "aws-agent-solution-architect"


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
          status: str = "active", spec: dict | None = None,
          system_key: str | None = None) -> str:
    db = SessionLocal()
    try:
        row = Agent(workspace_id=DEFAULT_WORKSPACE_ID, name=f"a-{method}", method=method,
                    status=status, system_key=system_key,
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
        pages=[{"events": [_event("[runtime-logs]1111-2222",
                                  json.dumps({"message": f"handling {SID}", "sessionId": SID}))]}],
    )
    agent_id = _seed(method="harness", resource_id="hr_harness-XyZ987")
    body = client.get(f"/api/agents/{agent_id}/runtime-logs",
                      params={"session_id": SID}).json()
    assert body["log_group"] == backing
    assert body["session_match"] == "content"
    assert body["events"][0]["session_id"] == SID
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


def test_harness_private_sessions_are_filtered_by_app_log_session_id(client, logs, monkeypatch):
    """A Harness names no stream after a session: the session is only in the JSON
    app-log line's ``sessionId``, so another principal's lines must still drop."""
    monkeypatch.setattr(agents_router, "PrivateSessions", _Private)
    backing = "/aws/bedrock-agentcore/runtimes/harness_hr_harness-Zz9-DEFAULT"
    stream = "2026/10/19/[runtime-logs]cccc"
    logs(
        groups=[{"logGroupName": backing, "creationTime": 2}],
        streams=[_stream(stream)],
        pages=[{"events": [
            _event(stream, json.dumps({"message": "mine", "sessionId": SID})),
            _event(stream, json.dumps({"message": "theirs", "sessionId": OTHER})),
            _event(stream, "plain stdout line"),
        ]}],
    )
    agent_id = _seed(method="harness", resource_id="hr_harness-XyZ987", system_key=PRESET)
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["session_id"] for e in body["events"]] == [SID]
    assert all("theirs" not in e["message"] for e in body["events"])
    assert body["hidden_unattributed"] == 1  # the plain line proves no ownership


HARNESS_GROUP = "/aws/bedrock-agentcore/runtimes/harness_hr_harness-Zz9-DEFAULT"
HARNESS_STREAM = "2026/10/19/[runtime-logs]cccc"


def _app_log(message: str, sid: Any) -> str:
    return json.dumps({"message": message, "sessionId": sid})


def _harness(logs, pages: list[dict], system_key: str | None = PRESET) -> tuple[StubLogs, str]:
    """A Harness agent; by default the preset, whose unattributed lines hide."""
    stub = logs(
        groups=[{"logGroupName": HARNESS_GROUP, "creationTime": 2}],
        streams=[_stream(HARNESS_STREAM)],
        pages=pages,
    )
    return stub, _seed(method="harness", resource_id="hr_harness-XyZ987",
                       system_key=system_key)


def test_harness_unattributable_lines_are_hidden_and_counted(client, logs):
    """A line of a stream named after no session proves no ownership: print-style
    stdout of the preset could be another principal's private session, so it drops."""
    _, agent_id = _harness(logs, [{"events": [
        _event(HARNESS_STREAM, _app_log("mine", SID)),
        _event(HARNESS_STREAM, "plain stdout line"),
        _event(HARNESS_STREAM, '{"message": "no session field"}'),
    ]}])
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["session_id"] for e in body["events"]] == [SID]
    assert body["hidden_unattributed"] == 2


@pytest.mark.parametrize("placeholder", ["-", "", "   ", "null", "None", "NULL", None])
def test_placeholder_session_ids_are_unattributed(client, logs, placeholder):
    _, agent_id = _harness(logs, [{"events": [
        _event(HARNESS_STREAM, _app_log("placeholder", placeholder)),
        _event(HARNESS_STREAM, _app_log("mine", SID)),
    ]}])
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["message"] for e in body["events"]] == [_app_log("mine", SID)]
    assert body["hidden_unattributed"] == 1


def test_harness_session_filter_keeps_only_lines_proven_to_be_that_session(client, logs):
    _, agent_id = _harness(logs, [{"events": [
        _event(HARNESS_STREAM, _app_log(f"handling {SID}", SID)),
        _event(HARNESS_STREAM, f"print mentioning {SID}"),
    ]}])
    body = client.get(f"/api/agents/{agent_id}/runtime-logs",
                      params={"session_id": SID}).json()
    assert body["session_match"] == "content"
    assert [e["session_id"] for e in body["events"]] == [SID]
    assert body["hidden_unattributed"] == 1


def test_session_named_streams_keep_their_plain_lines(client, logs):
    logs(
        streams=[_stream(SESSION_STREAM)],
        pages=[{"events": [_event(SESSION_STREAM, "plain stdout line"),
                           _event(SESSION_STREAM, _app_log("x", "-"))]}],
    )
    agent_id = _seed()
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["session_id"] for e in body["events"]] == [SID, SID]
    assert body["hidden_unattributed"] == 0


def test_dropped_lines_do_not_stall_paging(client, logs):
    """A page of only unattributable lines reads on, and the AWS token survives
    so the next page continues where the scan stopped."""
    stub, agent_id = _harness(logs, [
        {"events": [_event(HARNESS_STREAM, "noise")] * 3, "nextToken": "aws-1"},
        {"events": [_event(HARNESS_STREAM, _app_log("mine", SID))], "nextToken": "aws-2"},
        {"events": [_event(HARNESS_STREAM, "noise")]},
    ])
    body = client.get(f"/api/agents/{agent_id}/runtime-logs", params={"limit": 1}).json()
    assert [e["session_id"] for e in body["events"]] == [SID]
    assert body["hidden_unattributed"] == 3
    assert body["next_token"]
    assert [f.get("nextToken") for f in stub.filters()] == [None, "aws-1"]
    again = client.get(f"/api/agents/{agent_id}/runtime-logs",
                       params={"next_token": body["next_token"]}).json()
    assert stub.filters()[-1]["nextToken"] == "aws-2"
    assert again["events"] == [] and again["hidden_unattributed"] == 1
    assert again["next_token"] is None


def test_an_agent_without_private_sessions_shows_unattributed_lines(client, logs):
    """Private sessions run only on the preset: any other Harness's print-style
    stdout cannot be one, so it shows and nothing is counted as hidden."""
    _, agent_id = _harness(logs, [{"events": [
        _event(HARNESS_STREAM, _app_log("mine", SID)),
        _event(HARNESS_STREAM, "plain stdout line"),
    ]}], system_key=None)
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert [e["message"] for e in body["events"]] == [_app_log("mine", SID), "plain stdout line"]
    assert [e["session_id"] for e in body["events"]] == [SID, None]
    assert body["hidden_unattributed"] == 0


def test_the_preset_a_conversation_was_sent_to_hides_unattributed_lines(client, logs):
    # a conversation's preset row hides them even if it carries no system_key (any more)
    _, agent_id = _harness(logs, [{"events": [_event(HARNESS_STREAM, "plain stdout line")]}],
                           system_key=None)
    db = SessionLocal()
    try:
        db.add(AssistantConversation(workspace_id=DEFAULT_WORKSPACE_ID, owner="alice",
                                     owner_principal="user:alice", preset_agent_id=agent_id))
        db.commit()
    finally:
        db.close()
    body = client.get(f"/api/agents/{agent_id}/runtime-logs").json()
    assert body["events"] == [] and body["hidden_unattributed"] == 1


@pytest.mark.parametrize("system_key, expected", [(PRESET, None), (None, 0)])
def test_a_keyword_withholds_the_hidden_count(client, logs, system_key, expected):
    """Under a keyword the count of hidden lines would tell which of them contain it
    (a substring oracle on another principal's output), so the preset withholds it."""
    _, agent_id = _harness(logs, [{"events": [
        _event(HARNESS_STREAM, "plain stdout line with secret"),
    ]}], system_key=system_key)
    body = client.get(f"/api/agents/{agent_id}/runtime-logs",
                      params={"keyword": "secret"}).json()
    assert body["hidden_unattributed"] == expected
    assert len(body["events"]) == (0 if system_key else 1)
