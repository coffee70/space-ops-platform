"""Document knowledge routes."""

from fastapi import APIRouter

from app.routes.handlers import document_knowledge as handlers

router = APIRouter()
router.add_api_route('/documents', handlers.list_documents, methods=['GET'])
router.add_api_route('/documents', handlers.create_document, methods=['POST'])
router.add_api_route('/documents/{document_id}', handlers.get_document, methods=['GET'])
router.add_api_route('/documents/{document_id}', handlers.delete_document, methods=['DELETE'])
router.add_api_route('/documents/{document_id}/reingest', handlers.reingest_document, methods=['POST'])
router.add_api_route('/documents/search', handlers.search_documents, methods=['POST'])
router.add_api_route('/documents/{document_id}/chunks', handlers.list_document_chunks, methods=['GET'])
