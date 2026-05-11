"""Model-config admin service for models.local.yaml (validation delegated to agent-runtime)."""

from platform_common.web import create_service_app
from app.routes import model_config as model_config_routes

app = create_service_app(
    title="Model Config Service",
    description="Read, validate via agent-runtime, and save shared model registry YAML.",
)
app.include_router(model_config_routes.router, prefix="/model-config", tags=["model-config"])
