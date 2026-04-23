"""Vehicle configuration service entrypoint."""

from platform_common.web import create_service_app
from app.routes import vehicle_configs

app = create_service_app(
    title="Vehicle Config Service",
    description="Vehicle configuration management service.",
)
app.include_router(vehicle_configs.router, prefix="/vehicle-configs", tags=["vehicle-configs"])
