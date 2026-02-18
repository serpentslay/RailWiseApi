from fastapi import FastAPI
from app.api.v1.routes.health import router as health_router

app = FastAPI(title="RailWise MVP API")
app.include_router(health_router, prefix="/v1")