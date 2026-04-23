"""Telemetry query service routes."""

from fastapi import APIRouter

from app.routes import _telemetry_handlers as handlers

router = APIRouter()

router.add_api_route("/recompute-stats", handlers.recompute_stats, methods=["POST"])
router.add_api_route("/overview", handlers.overview, methods=["GET"])
router.add_api_route("/anomalies", handlers.anomalies, methods=["GET"])
router.add_api_route("/watchlist", handlers.list_watchlist, methods=["GET"])
router.add_api_route("/watchlist", handlers.add_watchlist, methods=["POST"])
router.add_api_route("/watchlist/{name}", handlers.delete_watchlist, methods=["DELETE"])
router.add_api_route("/list", handlers.list_telemetry, methods=["GET"])
router.add_api_route("/inventory", handlers.list_telemetry_inventory, methods=["GET"])
router.add_api_route("/subsystems", handlers.list_subsystems, methods=["GET"])
router.add_api_route("/units", handlers.list_units, methods=["GET"])
router.add_api_route("/{name}/streams", handlers.get_channel_streams, methods=["GET"])
router.add_api_route("/{name}/recent", handlers.get_recent, methods=["GET"])
router.add_api_route(
    "/sources/{source_id}/channels/{name}/recent",
    handlers.get_recent_for_source,
    methods=["GET"],
)
router.add_api_route(
    "/sources/{source_id}/channels/{name}/streams",
    handlers.get_channel_streams_for_source,
    methods=["GET"],
)
