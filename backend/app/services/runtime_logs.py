"""Agent detail · 运行日志 — the stdout/stderr of an agent's AgentCore Runtime.

AgentCore writes a runtime's container output to
``/aws/bedrock-agentcore/runtimes/<runtimeId>-<endpoint>`` (Launchpad reads the
``DEFAULT`` endpoint): one stream per session,
``YYYY/MM/DD/[runtime-logs-<sessionId>]<uuid>``, beside the shared OTel stream
``otel-rt-logs``. A Harness's backing runtime names its streams per microVM
(``[runtime-logs]<uuid>``, no session id) and its group carries the BACKING
runtime's id, discovered by prefix as batch evaluation does.

The log group is always derived here from the ledger row — a caller never names
one. A group that does not exist yet or a denied read degrades to
``unavailable_reason`` (the card explains it) instead of an error.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError

from app.core.errors import AppError, aws_error_code
from app.evaluation import log_streams
from app.evaluation.service import harness_log_group
from app.models.ledger import Agent
from app.services.agent_versions import DEFAULT_ENDPOINT, resource_kind
from app.services.observability import RANGE_HOURS, RUNTIME_LOG_GROUP_PREFIX
from app.services.traces import _log_group_url
from app.services.workspace import WorkspaceContext

MAX_LIMIT = 200
_DENIED = {"AccessDeniedException", "AccessDenied", "UnauthorizedOperation"}


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _pack(token: str, since_ms: int, until_ms: int) -> str:
    """The page token pins the window the first page chose, so ``range`` is not
    re-anchored to "now" while paging (FilterLogEvents tokens need the same
    request)."""
    raw = json.dumps({"t": token, "s": since_ms, "e": until_ms}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _unpack(value: str) -> tuple[str, int, int]:
    try:
        data = json.loads(base64.urlsafe_b64decode(value.encode()))
        token, since_ms, until_ms = data["t"], int(data["s"]), int(data["e"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise AppError(
            "runtime_logs.bad_token", "invalid next_token", status_code=422
        ) from exc
    if not isinstance(token, str) or not token:
        raise AppError("runtime_logs.bad_token", "invalid next_token", status_code=422)
    return token, since_ms, until_ms


def log_group_of(agent: Agent, logs: Any) -> str | None:
    """The agent's runtime log group, or None when it has no AgentCore Runtime
    (no resource yet, deleted, or a method that is neither Runtime nor Harness).
    A Harness group exists only after its first session; before that the
    lookup answers ``""`` (reported as ``log_group_not_found``)."""
    if agent.status == "deleted" or not agent.resource_id:
        return None
    kind = resource_kind(agent)
    if kind == "runtime":
        return f"{RUNTIME_LOG_GROUP_PREFIX}{agent.resource_id}-{DEFAULT_ENDPOINT}"
    if kind == "harness":
        return harness_log_group(agent.resource_id, logs) or ""
    return None


def agent_runtime_logs(
    agent: Agent,
    workspace: WorkspaceContext,
    *,
    range: str = "1h",
    session_id: str | None = None,
    keyword: str | None = None,
    include_otel: bool = False,
    next_token: str | None = None,
    limit: int = 100,
    visible: Any = None,
    logs: Any = None,
) -> dict[str, Any]:
    logs = logs or workspace.client("logs")
    out: dict[str, Any] = {
        "agent_id": agent.id,
        "range": range,
        "session_id": session_id,
        "keyword": (keyword or "").strip() or None,
        "include_otel": include_otel,
        "log_group": None,
        "cloudwatch_url": None,
        "events": [],
        "next_token": None,
        "session_match": None,
        "sessions": [],
        "hidden_unattributed": 0,
        "unavailable_reason": None,
    }
    if next_token:
        token, since_ms, until_ms = _unpack(next_token)
    else:
        token, until_ms = None, _now_ms()
        since_ms = until_ms - RANGE_HOURS[range] * 3600 * 1000
    try:
        log_group = log_group_of(agent, logs)
        if log_group is None:
            return {**out, "unavailable_reason": "no_runtime"}
        if not log_group:
            return {**out, "unavailable_reason": "log_group_not_found"}
        out["log_group"] = log_group
        out["cloudwatch_url"] = _log_group_url(workspace.region, log_group)
        page = log_streams.runtime_events(
            logs, log_group,
            since_ms=since_ms, until_ms=until_ms, limit=min(limit, MAX_LIMIT),
            session_id=session_id, keyword=keyword, include_otel=include_otel,
            token=token, visible=visible,
        )
        if not next_token and not session_id:
            # the session picker: sessions that own a stream in the window
            out["sessions"] = [
                sid for sid in log_streams.recent_sessions(logs, log_group, since_ms)
                if visible is None or visible(sid)
            ]
    except ClientError as exc:
        code = aws_error_code(exc)
        if code == "ResourceNotFoundException":
            return {**out, "unavailable_reason": "log_group_not_found"}
        if code in _DENIED:
            return {**out, "unavailable_reason": "access_denied"}
        raise
    return {
        **out,
        "events": page["events"],
        "session_match": page["session_match"],
        "hidden_unattributed": page["hidden_unattributed"],
        "next_token": _pack(page["next_token"], since_ms, until_ms)
        if page["next_token"] else None,
    }
