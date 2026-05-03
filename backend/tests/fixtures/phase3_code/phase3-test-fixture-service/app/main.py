from fastapi import FastAPI

app = FastAPI(title="Phase 3 Test Fixture Service")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metadata")
def metadata():
    return {"service": "phase3-test-fixture-service", "mode": "deterministic"}
