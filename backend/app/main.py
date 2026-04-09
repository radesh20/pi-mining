import logging
import logging.config
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
from app.middleware.request_id import RequestIDMiddleware, get_logging_config
from app.services.data_cache_service import get_data_cache_service
from app.config import settings

# ── Structured logging with request-ID correlation ───────────────────────────
logging.config.dictConfig(get_logging_config(level="INFO"))
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Process Mining AI Agents",
    description="Celonis Process Mining + Azure OpenAI",
    version="1.0.0",
)

app.add_middleware(RequestIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(process_router,    prefix="/api", tags=["Process"])
app.include_router(prompts_router,    prefix="/api", tags=["Prompts"])
app.include_router(agents_router,     prefix="/api", tags=["Agents"])
app.include_router(exceptions_router, prefix="/api", tags=["Exceptions"])
app.include_router(vendors_router,    prefix="/api", tags=["Vendors"])
app.include_router(chat_router,       prefix="/api", tags=["Chat"])


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
            "data_model": svc.data_model.name if svc.data_model else "N/A",
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={
            "status": "unhealthy",
            "celonis_connected": False,
            "error": str(e),
        })


@app.on_event("startup")
def preload_data_cache() -> None:
    """
    Kick off a background cache warm-up so the first chat request is never
    blocked waiting for Celonis data.

    FIX: Properly gate the flag BEFORE spawning the thread so there is no
    window where _refresh_in_progress is False while the thread is already
    running.  _refresh_in_background resets the flag in its own finally
    block, so we must NOT set it here a second time — just let the method
    own its lifecycle.
    """
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        module=r"pycelonis\.utils\.deprecation",
    )

    if getattr(settings, "CACHE_ENABLE_STARTUP_WARMUP", True):

        def _warm_cache_once() -> None:
            try:
                cache = get_data_cache_service()

                # Gate: only start if not already running or loaded.
                with cache._lock:
                    if cache._refresh_in_progress or cache._is_loaded:
                        logger.info(
                            "Startup warmup skipped: already loading or loaded."
                        )
                        return
                    # Claim the lock atomically inside the lock.
                    cache._refresh_in_progress = True
                    cache._refresh_started_at = time.time()

                # Run refresh — _refresh_in_background owns the finally/reset.
                # But since we already set the flag above, call _refresh_all_data_impl
                # directly and handle cleanup ourselves so the flag is never double-set.
                try:
                    cache._refresh_all_data_impl()
                    logger.info("Startup cache warm-up completed successfully.")
                except Exception as exc:
                    with cache._lock:
                        cache.last_error = str(exc)
                    logger.error(
                        "Startup cache warm-up _refresh_all_data_impl failed: %s",
                        str(exc),
                        exc_info=True,
                    )
                finally:
                    with cache._lock:
                        cache._refresh_in_progress = False
                        cache._refresh_started_at = None
                        cache._refresh_cond.notify_all()
                    logger.info("Startup cache warm-up lock released.")

            except Exception as exc:
                logger.error(
                    "Startup cache warmup outer failure: %s", str(exc), exc_info=True
                )

        threading.Thread(
            target=_warm_cache_once,
            daemon=True,
            name="cache-startup-warmup",
        ).start()
        logger.info("Background cache warm-up thread spawned.")

    # ── Optional periodic auto-refresh ───────────────────────────────────────
    refresh_seconds = max(
        int(getattr(settings, "CACHE_AUTO_REFRESH_SECONDS", 0) or 0), 0
    )
    if refresh_seconds <= 0:
        return

    def _auto_refresh_loop() -> None:
        cache = get_data_cache_service()
        policy = str(
            getattr(settings, "CACHE_AUTO_REFRESH_POLICY", "stale_only") or "stale_only"
        ).lower()
        while True:
            time.sleep(refresh_seconds)
            try:
                if policy == "stale_only" and not cache.is_stale():
                    continue
                cache.refresh_all_data()
            except Exception as exc:
                logger.warning("Background cache refresh failed: %s", str(exc))

    threading.Thread(
        target=_auto_refresh_loop,
        daemon=True,
        name="cache-auto-refresh",
    ).start()