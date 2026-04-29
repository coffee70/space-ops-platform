"""Code intelligence routes."""

from fastapi import APIRouter

from app.routes.handlers import code_intelligence as handlers

router = APIRouter()
router.add_api_route("/repositories", handlers.list_repositories, methods=["GET"])
router.add_api_route("/repositories/index", handlers.index_repository, methods=["POST"])
router.add_api_route("/repositories/{repository_id}/status", handlers.get_repository_status, methods=["GET"])
router.add_api_route("/search", handlers.search_code, methods=["POST"])
router.add_api_route("/source-file", handlers.read_source_file, methods=["GET"])
router.add_api_route("/related-context", handlers.related_context, methods=["POST"])
