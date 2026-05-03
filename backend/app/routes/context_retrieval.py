"""Context retrieval routes."""

from fastapi import APIRouter, Depends, Request

from app.intelligence.clients.context_retrieval_clients import get_context_clients
from app.routes.handlers import context_retrieval as handlers

router = APIRouter()


def context_packet(body: dict, request: Request, clients=Depends(get_context_clients)):
    return handlers.context_packet(body, request, clients)


router.add_api_route('/packet', context_packet, methods=['POST'])
