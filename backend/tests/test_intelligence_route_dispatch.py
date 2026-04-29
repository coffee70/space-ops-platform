"""Ensure root-local intelligence routers register static paths before dynamic ones."""

from __future__ import annotations

import sys
import types

# Importing handlers pulls embedding_service -> sentence_transformers; stub for CI/light envs.
if "sentence_transformers" not in sys.modules:
    _st = types.ModuleType("sentence_transformers")

    class _SentenceTransformer:  # noqa: N801
        def __init__(self, *_args, **_kwargs):
            pass

        def encode(self, *_args, **_kwargs):
            import numpy as np  # noqa: PLC0415

            return np.array([0.0])

    _st.SentenceTransformer = _SentenceTransformer
    sys.modules["sentence_transformers"] = _st

# document_knowledge routes use File/Form; allow router import without python-multipart installed.
import fastapi.dependencies.utils as _fastapi_dep_utils

_fastapi_dep_utils.ensure_multipart_is_installed = lambda: None  # type: ignore[method-assign]

from fastapi.routing import APIRoute

from app.routes import code_intelligence, document_knowledge, tool_registry


def _api_routes(router) -> list[APIRoute]:
    return [r for r in router.routes if isinstance(r, APIRoute)]


def _route_index(routes: list[APIRoute], *, path: str, method: str) -> int:
    for i, route in enumerate(routes):
        if route.path == path and method in route.methods:
            return i
    raise AssertionError(f"no route {method} {path!r} in {[ (r.path, sorted(r.methods)) for r in routes ]}")


def test_document_knowledge_static_before_dynamic_document_id() -> None:
    routes = _api_routes(document_knowledge.router)
    i_post_search = _route_index(routes, path="/search", method="POST")
    i_get_chunks = _route_index(routes, path="/{document_id}/chunks", method="GET")
    i_post_reingest = _route_index(routes, path="/{document_id}/reingest", method="POST")
    i_get_doc = _route_index(routes, path="/{document_id}", method="GET")
    _ = _route_index(routes, path="/{document_id}", method="DELETE")
    assert i_post_search < i_get_doc
    assert i_get_chunks < i_get_doc
    assert i_post_reingest < i_get_doc


def test_code_intelligence_repositories_index_before_status_param() -> None:
    routes = _api_routes(code_intelligence.router)
    i_list = _route_index(routes, path="/repositories", method="GET")
    i_index = _route_index(routes, path="/repositories/index", method="POST")
    i_status = _route_index(routes, path="/repositories/{repository_id}/status", method="GET")
    assert i_list < i_index < i_status


def test_tool_registry_definitions_seed_before_tool_name_param() -> None:
    routes = _api_routes(tool_registry.router)
    i_list = _route_index(routes, path="/definitions", method="GET")
    i_seed = _route_index(routes, path="/definitions/seed", method="POST")
    i_get_tool = _route_index(routes, path="/definitions/{tool_name}", method="GET")
    _ = _route_index(routes, path="/definitions/{tool_name}", method="PATCH")
    assert i_list < i_seed < i_get_tool
