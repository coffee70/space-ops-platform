"""Shared FastAPI application factory and middleware."""

from __future__ import annotations

import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import get_settings
from app.lib.audit import audit_log
from app.lib.logging_setup import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

LifespanFactory = Callable[[FastAPI], Awaitable[None]]


def _cors_settings() -> tuple[list[str], str, re.Pattern[str] | None]:
    settings = get_settings()
    origins = settings.get_cors_origins_list()
    raw_regex = (settings.cors_origin_regex or "").strip()
    pattern = re.compile(raw_regex) if raw_regex else None
    return origins, raw_regex, pattern


def _allowed_request_origin(origin: str | None, *, origins: list[str], pattern: re.Pattern[str] | None) -> str:
    if not origin:
        return origins[0] if origins else ""
    if origin in origins:
        return origin
    if pattern and pattern.fullmatch(origin):
        return origin
    return origins[0] if origins else ""


def _cors_headers(request: Request, *, origins: list[str], pattern: re.Pattern[str] | None) -> dict[str, str]:
    allowed = _allowed_request_origin(request.headers.get("origin"), origins=origins, pattern=pattern)
    return {
        "Access-Control-Allow-Origin": allowed,
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
    }


def create_service_app(
    *,
    title: str,
    description: str,
    version: str = "1.0.0",
    lifespan: Callable[[FastAPI], object] | None = None,
) -> FastAPI:
    """Create a service app with consistent middleware and error handling."""

    origins, raw_regex, pattern = _cors_settings()

    if lifespan is None:
        @asynccontextmanager
        async def default_lifespan(_: FastAPI):
            yield

        lifespan = default_lifespan

    app = FastAPI(title=title, description=description, version=version, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_origin_regex=raw_regex or None,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_with_cors(request: Request, exc: StarletteHTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=_cors_headers(request, origins=origins, pattern=pattern),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_with_cors(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
            headers=_cors_headers(request, origins=origins, pattern=pattern),
        )

    @app.exception_handler(Exception)
    async def add_cors_to_exception_response(request: Request, exc: Exception):
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
            headers=_cors_headers(request, origins=origins, pattern=pattern),
        )

    @app.middleware("http")
    async def audit_request_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id
        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000
        audit_log(
            "http.request",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2),
            request_id=request_id,
        )
        try:
            response.headers["X-Request-ID"] = request_id
        except (TypeError, ValueError):
            pass
        return response

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
