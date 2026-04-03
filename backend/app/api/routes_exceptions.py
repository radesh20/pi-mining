import asyncio
import threading

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisConnectionError
from app.services.exception_workbench_service import ExceptionWorkbenchService
from app.services.teams_service import TeamsWebhookService
from app.services.data_cache_service import get_data_cache_service

router = APIRouter()


def _build_context() -> dict:
    cache = get_data_cache_service()
    return cache.get_process_context()


def _build_workbench(auto_notify_human_review: bool = True) -> ExceptionWorkbenchService:
    llm = AzureOpenAIService()
    teams = TeamsWebhookService()
    return ExceptionWorkbenchService(
        llm=llm,
        teams_service=teams,
        auto_notify_human_review=auto_notify_human_review,
    )


def _trigger_background_refresh_if_needed(cache) -> None:
    status = cache.get_cache_status()
    if status.get("refresh_in_progress", False):
        return
    threading.Thread(
        target=cache.refresh_all_data,
        daemon=True,
        name="exceptions-workbench-refresh-bg",
    ).start()


@router.get("/exceptions/categories")
def get_exception_categories():
    try:
        cache = get_data_cache_service()
        data = cache.get_exception_categories()
        return {"success": True, "data": data}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch exception categories: {str(e)}")


@router.get("/exceptions/records")
def get_exception_records(
    type: str = Query(..., description="Exception category, e.g., Payment Terms Mismatch"),
):
    try:
        cache = get_data_cache_service()
        data = cache.get_all_exception_records() if str(type).strip() == "*" else cache.get_exception_records(type)
        return {"success": True, "data": data}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch exception records: {str(e)}")


@router.get("/exceptions/workbench-data")
async def get_exception_workbench_data():
    """
    Returns categories + records for Exception Workbench.
    - Serves cached snapshot immediately when available.
    - Refreshes cache in background when stale.
    - Applies 10s timeout for load/refresh attempts.
    """
    try:
        cache = get_data_cache_service()
        snapshot = cache.get_exception_workbench_snapshot()
        has_cached_data = bool(snapshot.get("categories")) or bool(snapshot.get("records"))

        if has_cached_data:
            if snapshot.get("is_stale") and not snapshot.get("refresh_in_progress"):
                _trigger_background_refresh_if_needed(cache)
            return {
                "success": True,
                "data": {
                    "categories": snapshot.get("categories", []),
                    "records": snapshot.get("records", []),
                    "warning": (
                        "Serving cached data while refresh is in progress."
                        if snapshot.get("is_stale")
                        else None
                    ),
                    "served_from_cache": True,
                    "refresh_in_background": bool(snapshot.get("is_stale")),
                },
            }

        timeout_seconds = 10.0
        warning = None

        try:
            await asyncio.wait_for(asyncio.to_thread(cache.ensure_loaded), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            warning = "Cache warmup exceeded 10 seconds; returning available partial data."
            _trigger_background_refresh_if_needed(cache)
        except Exception:
            warning = "Cache warmup failed; returning available partial data."

        categories, records = await asyncio.gather(
            asyncio.to_thread(cache.get_exception_categories),
            asyncio.to_thread(cache.get_all_exception_records),
        )

        return {
            "success": True,
            "data": {
                "categories": categories or [],
                "records": records or [],
                "warning": warning,
                "served_from_cache": False,
                "refresh_in_background": warning is not None,
            },
        }
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch exception workbench data: {str(e)}")


@router.post("/exceptions/analyze")
def analyze_exception(payload: dict = Body(...)):
    try:
        process_context = _build_context()
        svc = _build_workbench(auto_notify_human_review=False)
        data = svc.analyze_exception(payload, process_context)
        return {"success": True, "data": data}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze exception: {str(e)}")


@router.post("/exceptions/next-best-action")
def next_best_action(payload: dict = Body(...)):
    try:
        process_context = _build_context()
        analysis = payload.get("analysis", {})
        if not isinstance(analysis, dict) or not analysis:
            raise HTTPException(status_code=400, detail="Body must include a non-empty 'analysis' object.")

        svc = _build_workbench(auto_notify_human_review=True)
        data = svc.next_best_action(analysis, process_context)
        return {"success": True, "data": data}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate next best action: {str(e)}")


@router.post("/exceptions/send-human-review")
def send_human_review(payload: dict = Body(...)):
    try:
        # Build context to satisfy workbench route contract and keep consistency.
        _ = _build_context()
        analysis = payload.get("analysis", {})
        if not isinstance(analysis, dict) or not analysis:
            raise HTTPException(status_code=400, detail="Body must include a non-empty 'analysis' object.")

        teams = TeamsWebhookService()
        data = teams.send_human_review_card(analysis)
        return {"success": True, "data": data}
    except CelonisConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send human review to Teams: {str(e)}")
