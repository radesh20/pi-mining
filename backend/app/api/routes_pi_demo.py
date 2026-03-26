"""
POST /pi-demo/run

Accepts a case_id, fetches real PI data from existing cache/services,
runs the PI Orchestrator, and returns a BI vs PI comparison response.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

router = APIRouter()


@router.post("/pi-demo/run")
def run_pi_demo(payload: dict = Body(...)):
    """
    PI-driven dynamic agent orchestration demo.

    Body parameters:
        case_id (str): invoice / case ID to look up in the cache.

    Returns:
        {
            bi_decision: str,
            pi_decision: str,
            agents_triggered: list[str],
            agent_outputs: list[dict],
            flow: str,
            final_action: str,
            reason: str,
            signals: dict,
        }
    """
    case_id: str = str(payload.get("case_id") or "").strip()
    if not case_id:
        raise HTTPException(status_code=422, detail="case_id is required")

    try:
        from app.services.data_cache_service import get_data_cache_service
        from app.services.pi_orchestrator_service import PIOrchestrator

        cache = get_data_cache_service()

        case_data = cache.get_invoice_case(case_id)
        if not case_data:
            raise HTTPException(
                status_code=404,
                detail=f"Case '{case_id}' not found in process data.",
            )

        process_context = cache.get_process_context()
        vendor_stats = cache.get_vendor_stats()

        orchestrator = PIOrchestrator(
            case_data=case_data,
            process_context=process_context,
            vendor_stats=vendor_stats,
        )
        result = orchestrator.run()
        return {"success": True, "case_id": case_id, "data": result}

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"PI demo orchestration failed: {str(exc)}",
        )
