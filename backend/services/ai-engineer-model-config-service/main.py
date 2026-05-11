"""AI Engineer model registry file editor service."""

from platform_common.web import create_service_app
from app.routes import ai_engineer_model_config

app = create_service_app(
    title="AI Engineer Model Config Service",
    description="Read, validate, and save agent runtime model registry YAML.",
)
app.include_router(ai_engineer_model_config.router, prefix="/model-config", tags=["ai-engineer-model-config"])
