from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import etl, health, scheduler
from app.config import settings
from app.database import engine
from app.etl.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if settings.ETL_SCHEDULE_ENABLED:
        start_scheduler()
    yield
    stop_scheduler()
    await engine.dispose()


app = FastAPI(
    title="MYAMZTEAM ERP Command Center",
    description="Automated daily P&L for MagicalButter",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(etl.router, prefix="/api", tags=["etl"])
app.include_router(scheduler.router, prefix="/api", tags=["scheduler"])


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "myamzteam-erp", "status": "running"}
