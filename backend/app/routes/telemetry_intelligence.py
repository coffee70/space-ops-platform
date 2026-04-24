"""Telemetry intelligence service routes."""

from fastapi import APIRouter

from app.routes.handlers import telemetry_intelligence as handlers

router = APIRouter()

router.add_api_route("/schema", handlers.create_schema, methods=["POST"])
router.add_api_route("/search", handlers.search, methods=["GET"])
router.add_api_route("/{name}/summary", handlers.get_summary, methods=["GET"])
router.add_api_route(
    "/sources/{source_id}/channels/{name}/summary",
    handlers.get_summary_for_source,
    methods=["GET"],
)
router.add_api_route("/{name}/explain", handlers.explain, methods=["GET"])
router.add_api_route(
    "/sources/{source_id}/channels/{name}/explain",
    handlers.explain_for_source,
    methods=["GET"],
)
