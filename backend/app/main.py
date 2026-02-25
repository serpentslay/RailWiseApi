from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.routes.health import router as health_router
from app.api.v1.routes.reliability import router as reliability_router


app = FastAPI(title="RailWise MVP API")

# Dev-friendly CORS policy: allow all origins/methods/headers so the frontend can call the API directly.
# Tighten this for production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="/v1")
app.include_router(reliability_router)
