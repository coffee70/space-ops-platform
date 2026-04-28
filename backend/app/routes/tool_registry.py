"""Tool registry routes."""

from fastapi import APIRouter

from app.routes.handlers import tool_registry as handlers

router = APIRouter()
router.add_api_route('/tools', handlers.list_tools, methods=['GET'])
router.add_api_route('/tools/{tool_name}', handlers.get_tool, methods=['GET'])
router.add_api_route('/tools/seed', handlers.seed_tools, methods=['POST'])
router.add_api_route('/tools/{tool_name}', handlers.patch_tool, methods=['PATCH'])
