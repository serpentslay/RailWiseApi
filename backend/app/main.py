from fastapi import FastAPI
from app.api.v1.routes.health import router as health_router
from app.api.v1.routes.reliability import router as reliability_router


app = FastAPI(title="RailWise MVP API")
app.include_router(health_router, prefix="/v1")
app.include_router(reliability_router)