import logging
import threading
import time
import warnings

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes_process import router as process_router
from app.api.routes_prompts import router as prompts_router
from app.api.routes_agents import router as agents_router
from app.api.routes_exceptions import router as exceptions_router
from app.api.routes_vendors import router as vendors_router
from app.api.routes_chat import router as chat_router

from app.services.data_cache_service import get_data_cache_service
from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Process Mining AI Agents",
    description="Celonis Process Mining + Azure OpenAI",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(process_router,    prefix="/api", tags=["Process"])
app.include_router(prompts_router,    prefix="/api", tags=["Prompts"])
app.include_router(agents_router,     prefix="/api", tags=["Agents"])
app.include_router(exceptions_router, prefix="/api", tags=["Exceptions"])
app.include_router(vendors_router,    prefix="/api", tags=["Vendors"])
app.include_router(chat_router,       prefix="/api", tags=["Chat"])

# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Process Mining AI Agents API is running"}

# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    from app.services.celonis_service import CelonisService
    try:
        svc = CelonisService()
        return {
            "status": "healthy",
            "celonis_connected": True,
            "data_model": svc.data_model.name if svc.data_model else "N/A"
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "celonis_connected": False,
                "error": str(e)
            }
        )

# ---------------------------------------------------------------------------
# Startup: Cache Warmup + Fingerprint Build
# ---------------------------------------------------------------------------

@app.on_event("startup")
def preload_data_cache() -> None:
    """
    Warm cache at startup so UI drilldowns do not trigger Celonis re-queries.
    This ALSO builds entity fingerprints (Phase 1).
    """

    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        module=r"pycelonis\.utils\.deprecation",
    )

    try:
        if settings.CACHE_ENABLE_STARTUP_WARMUP:
            logger.info("Starting cache warmup...")

            cache = get_data_cache_service()
            cache.ensure_loaded()   # THIS triggers fingerprint build

            logger.info("Cache warmup completed")

    except Exception as e:
        logger.error("Startup cache warmup failed: %s", str(e), exc_info=True)

    # -----------------------------------------------------------------------
    # Background Auto Refresh (optional)
    # -----------------------------------------------------------------------

    refresh_seconds = max(int(getattr(settings, "CACHE_AUTO_REFRESH_SECONDS", 0) or 0), 0)

    if refresh_seconds <= 0:
        return

    def _auto_refresh_loop() -> None:
        cache = get_data_cache_service()
        policy = str(getattr(settings, "CACHE_AUTO_REFRESH_POLICY", "stale_only") or "stale_only").lower()

        while True:
            time.sleep(refresh_seconds)
            try:
                if policy == "stale_only" and not cache.is_stale():
                    continue

                logger.info("Running background cache refresh...")
                cache.refresh_all_data()

            except Exception as e:
                logger.warning("Background cache refresh failed: %s", str(e))

    thread = threading.Thread(
        target=_auto_refresh_loop,
        daemon=True,
        name="cache-auto-refresh"
    )
    thread.start()