"""Source-registry-owned telemetry routes."""

from fastapi import APIRouter

from app.routes.handlers import source_registry as handlers

router = APIRouter()

router.add_api_route("/sources", handlers.list_sources, methods=["GET"])
router.add_api_route("/sources", handlers.create_source_route, methods=["POST"])
router.add_api_route("/sources/resolve", handlers.resolve_source_route, methods=["POST"])
router.add_api_route("/sources/{source_id}", handlers.update_source_route, methods=["PATCH"])
router.add_api_route(
    "/sources/{source_id}/backfill-progress",
    handlers.update_source_backfill_progress,
    methods=["POST"],
)
router.add_api_route(
    "/sources/{source_id}/live-state",
    handlers.update_source_live_state,
    methods=["POST"],
)
router.add_api_route(
    "/sources/{source_id}/observations/upcoming",
    handlers.get_upcoming_source_observations,
    methods=["GET"],
)
router.add_api_route(
    "/sources/{source_id}/observations/next",
    handlers.get_next_source_observation,
    methods=["GET"],
)
router.add_api_route(
    "/sources/{source_id}/observations:batch-upsert",
    handlers.batch_upsert_source_observations,
    methods=["POST"],
)
router.add_api_route(
    "/sources/{source_id}/streams",
    handlers.get_source_streams,
    methods=["GET"],
)
router.add_api_route("/sources/active-stream", handlers.set_active_stream, methods=["POST"])
