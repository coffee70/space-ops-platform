"""Tool execution routes."""

from fastapi import APIRouter

from app.routes.handlers import tool_execution as handlers

router = APIRouter()
router.add_api_route("/execute", handlers.execute_tool, methods=["POST"])
