"""Stable platform API gateway HTTP proxy routes."""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException, Request

from platform_common.service_proxy import proxy_request

router = APIRouter()


def _resolve_telemetry_service(path: str) -> tuple[str, str]:
    if path == "realtime/ingest" or path.startswith("feed-health"):
        return "telemetry-ingest-service", path
    if path.startswith("position/") or path.startswith("orbit/"):
        return "position-orbit-service", path
    if path.startswith("sources/") and "/channels/" in path:
        if path.endswith("/summary") or path.endswith("/explain"):
            return "telemetry-intelligence-service", path
        if path.endswith("/recent") or path.endswith("/streams"):
            return "telemetry-query-service", path
    if path.startswith("sources"):
        return "source-registry-service", path
    if path == "overview" or path == "anomalies" or path.startswith("watchlist") or path in {
        "list",
        "inventory",
        "subsystems",
        "units",
        "recompute-stats",
    }:
        return "telemetry-query-service", path
    if path == "schema" or path == "search" or path.endswith("/summary") or path.endswith("/explain"):
        return "telemetry-intelligence-service", path
    if path == "data" or path.endswith("/recent") or path.endswith("/streams") or "/channels/" in path:
        return "telemetry-query-service", path
    raise HTTPException(status_code=404, detail="Unknown telemetry route")


@router.api_route("/telemetry/{path:path}", methods=["GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"])
async def proxy_telemetry(request: Request, path: str):
    service_slug, target_path = _resolve_telemetry_service(path)
    return await proxy_request(service_slug, request, path=f"telemetry/{target_path}")


@router.api_route("/vehicle-configs", methods=["GET", "POST"])
@router.api_route("/vehicle-configs/{path:path}", methods=["GET", "PUT", "POST"])
async def proxy_vehicle_configs(request: Request, path: str = ""):
    return await proxy_request("vehicle-config-service", request, path=f"vehicle-configs/{path}")


@router.api_route("/simulator/{path:path}", methods=["GET", "POST"])
async def proxy_simulator(request: Request, path: str):
    return await proxy_request("simulator-control-service", request, path=f"simulator/{path}")


@router.api_route("/ops/events", methods=["GET"])
async def proxy_ops_events(request: Request):
    return await proxy_request("ops-events-service", request, path="ops/events")


@router.api_route("/ops/feed-status", methods=["GET"])
async def proxy_feed_status(request: Request):
    return await proxy_request("telemetry-ingest-service", request, path="telemetry/feed-health")


def _resolve_intelligence_service(path: str) -> tuple[str, str]:
    if path == "agent" or path.startswith("agent/"):
        return "agent-runtime-service", path[len("agent/") :] if path.startswith("agent/") else ""
    if path == "context" or path.startswith("context/"):
        return "context-retrieval-service", path[len("context/") :] if path.startswith("context/") else ""
    if path == "documents" or path == "documents/search":
        return "document-knowledge-service", path[len("documents/") :] if path.startswith("documents/") else ""
    if re.fullmatch(r"documents/[^/]+", path) or re.fullmatch(r"documents/[^/]+/chunks", path):
        return "document-knowledge-service", path[len("documents/") :]
    if path == "code/repositories" or path == "code/search" or path == "code/source-file" or path == "code/related-context":
        return "code-intelligence-service", path[len("code/") :]
    if re.fullmatch(r"code/repositories/[^/]+/status", path):
        return "code-intelligence-service", path[len("code/") :] if path.startswith("code/") else ""
    if path == "tools/execute" or path.startswith("tools/execute/"):
        return "tool-execution-service", path[len("tools/") :]
    if path == "tools/definitions" or path.startswith("tools/definitions/"):
        return "tool-registry-service", path[len("tools/") :]
    raise HTTPException(status_code=404, detail="Unknown intelligence route")


@router.api_route("/intelligence/{path:path}", methods=["GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"])
async def proxy_intelligence(request: Request, path: str):
    service_slug, target_path = _resolve_intelligence_service(path)
    return await proxy_request(service_slug, request, path=target_path)
