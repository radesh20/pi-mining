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
from app.api.routes_pi_demo import router as pi_demo_router
from app.services.data_cache_service import get_data_cache_service
from app.config import settings

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Process Mining AI Agents",
    description="Celonis Process Mining + Azure OpenAI",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(process_router, prefix="/api", tags=["Process"])
app.include_router(prompts_router, prefix="/api", tags=["Prompts"])
app.include_router(agents_router, prefix="/api", tags=["Agents"])
app.include_router(exceptions_router, prefix="/api", tags=["Exceptions"])
app.include_router(vendors_router, prefix="/api", tags=["Vendors"])
app.include_router(pi_demo_router, prefix="/api", tags=["PI Demo"])


@app.get("/")
def root():
    return {"message": "Process Mining AI Agents API is running"}


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
        return JSONResponse(status_code=503, content={
            "status": "unhealthy",
            "celonis_connected": False,
            "error": str(e)
        })


@app.on_event("startup")
def preload_data_cache() -> None:
    """
    Warm cache at startup so UI drilldowns do not trigger Celonis re-queries.
    """
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        module=r"pycelonis\.utils\.deprecation",
    )
    try:
        if settings.CACHE_ENABLE_STARTUP_WARMUP:
            def _warm_cache_once() -> None:
                try:
                    cache = get_data_cache_service()
                    cache.ensure_loaded()
                except Exception as e:  # noqa: BLE001
                    logger.warning("Startup cache warmup failed (non-blocking): %s", str(e))

            threading.Thread(target=_warm_cache_once, daemon=True, name="cache-startup-warmup").start()
    except Exception:
        # Do not block API startup; cache can still be refreshed manually.
        pass

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
                cache.refresh_all_data()
            except Exception as e:  # noqa: BLE001
                logger.warning("Background cache refresh failed: %s", str(e))

    t = threading.Thread(target=_auto_refresh_loop, daemon=True, name="cache-auto-refresh")
    t.start()
