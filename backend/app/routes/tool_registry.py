"""Tool registry routes."""

from fastapi import APIRouter

from app.routes.handlers import tool_registry as handlers

router = APIRouter()
# /definitions/seed before /definitions/{tool_name} (see route ordering guard).
router.add_api_route("/definitions", handlers.list_tools, methods=["GET"])
router.add_api_route("/definitions/seed", handlers.seed_tools, methods=["POST"])
router.add_api_route("/definitions/{tool_name}", handlers.get_tool, methods=["GET"])
