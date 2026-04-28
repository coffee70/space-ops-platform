"""Stable platform API gateway HTTP proxy routes."""

from __future__ import annotations

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
    if path.startswith("agent/"):
        return "agent-runtime-service", path[len("agent/") :]
    if path == "agent":
        return "agent-runtime-service", ""
    if path.startswith("context/"):
        return "context-retrieval-service", path[len("context/") :]
    if path == "context":
        return "context-retrieval-service", ""
    if path.startswith("documents/") or path == "documents":
        suffix = path[len("documents/") :] if path.startswith("documents/") else ""
        return "document-knowledge-service", suffix
    if path.startswith("code/") or path == "code":
        suffix = path[len("code/") :] if path.startswith("code/") else ""
        return "code-intelligence-service", suffix
    if path.startswith("tools/execute"):
        suffix = path[len("tools/execute") :].lstrip("/")
        return "tool-execution-service", suffix
    if path.startswith("tools/") or path == "tools":
        suffix = path[len("tools/") :] if path.startswith("tools/") else ""
        return "tool-registry-service", suffix
    raise HTTPException(status_code=404, detail="Unknown intelligence route")


@router.api_route("/intelligence/{path:path}", methods=["GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"])
async def proxy_intelligence(request: Request, path: str):
    service_slug, target_path = _resolve_intelligence_service(path)
    return await proxy_request(service_slug, request, path=target_path)
