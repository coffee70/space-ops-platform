"""Document knowledge routes."""

from fastapi import APIRouter

from app.routes.handlers import document_knowledge as handlers

router = APIRouter()
# Static routes before dynamic /{document_id} (see route ordering guard).
router.add_api_route("/", handlers.list_documents, methods=["GET"])
router.add_api_route("/", handlers.create_document, methods=["POST"])
router.add_api_route("/search", handlers.search_documents, methods=["POST"])
router.add_api_route("/{document_id}/chunks", handlers.list_document_chunks, methods=["GET"])
router.add_api_route("/{document_id}/reingest", handlers.reingest_document, methods=["POST"])
router.add_api_route("/{document_id}", handlers.get_document, methods=["GET"])
router.add_api_route("/{document_id}", handlers.delete_document, methods=["DELETE"])
