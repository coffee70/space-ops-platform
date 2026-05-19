"""Tool execution routes."""

from fastapi import APIRouter, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.routes.handlers import tool_execution as handlers


_TRACE_UUID_FIELDS = {"conversation_id", "agent_run_id", "request_id", "tool_call_id"}


async def tool_execution_validation_exception_handler(
    _request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    issues: list[dict[str, str]] = []
    invalid_uuid_fields: list[str] = []
    for issue in exc.errors():
        location = tuple(str(part) for part in issue.get("loc", ()))
        field_name = location[1] if len(location) >= 2 and location[0] == "body" else None
        issue_type = str(issue.get("type", "validation_error"))
        issues.append(
            {
                "field": ".".join(location),
                "message": str(issue.get("msg", "Invalid value.")),
                "type": issue_type,
            }
        )
        if field_name in _TRACE_UUID_FIELDS and "uuid" in issue_type:
            invalid_uuid_fields.append(field_name)

    if invalid_uuid_fields:
        unique_fields = list(dict.fromkeys(invalid_uuid_fields))
        message = (
            f"{unique_fields[0]} must be a valid UUID."
            if len(unique_fields) == 1
            else f"{', '.join(unique_fields)} must be valid UUIDs."
        )
        error_code = "invalid_tool_execution_trace_id"
    else:
        message = "Tool execution request failed validation."
        error_code = "invalid_tool_execution_request"

    return JSONResponse(
        status_code=400,
        content={
            "detail": {
                "error_code": error_code,
                "message": message,
                "issues": issues,
            }
        },
    )

router = APIRouter()
router.add_api_route("/execute", handlers.execute_tool, methods=["POST"])
