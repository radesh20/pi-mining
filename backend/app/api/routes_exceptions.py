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


def _build_workbench(auto_notify_human_review: bool = False) -> ExceptionWorkbenchService:
    llm = AzureOpenAIService()
    teams = TeamsWebhookService()
    return ExceptionWorkbenchService(
        llm=llm,
        teams_service=teams,
        auto_notify_human_review=auto_notify_human_review,
    )


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

        svc = _build_workbench(auto_notify_human_review=False)
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
