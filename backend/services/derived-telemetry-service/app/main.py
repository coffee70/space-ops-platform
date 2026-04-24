from fastapi import FastAPI

app = FastAPI(title="Derived Telemetry Service")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "derived-telemetry-service"}


@app.get("/")
def root() -> dict:
    return {
        "capability": "derived-telemetry",
        "operations": ["summarize", "rollup", "health"],
    }

