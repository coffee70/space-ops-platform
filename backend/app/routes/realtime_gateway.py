"""Gateway-owned realtime websocket routes."""

from fastapi import APIRouter

from app.routes.realtime import websocket_realtime

router = APIRouter()
router.add_api_websocket_route("/ws", websocket_realtime)
