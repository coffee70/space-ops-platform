"""Context retrieval routes."""

from fastapi import APIRouter

from app.routes.handlers import context_retrieval as handlers

router = APIRouter()
router.add_api_route('/packet', handlers.context_packet, methods=['POST'])
