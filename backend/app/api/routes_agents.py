from fastapi import APIRouter, Body, HTTPException
from app.services.data_cache_service import get_data_cache_service

router = APIRouter()


def _build_context_and_llm():
    """
    Lazy import all backend services to avoid startup-time import failures.
    """
    from app.services.azure_openai_service import AzureOpenAIService

    cache = get_data_cache_service()
    process_context = cache.get_process_context()
    llm = AzureOpenAIService()
    return process_context, llm


@router.post("/agents/execute-invoice")
def execute_invoice_flow(payload: dict = Body(...)):
    """
    Full 6-agent orchestration entry point.
    """
    try:
        process_context, llm = _build_context_and_llm()
        from app.services.orchestrator_service import OrchestratorService

        orchestrator = OrchestratorService(llm, process_context)
        result = orchestrator.execute_invoice_flow(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute full invoice orchestration: {str(e)}",
        )


@router.post("/agents/vendor-intelligence")
def vendor_intelligence(payload: dict = Body(...)):
    try:
        cache = get_data_cache_service()
        process_context, llm = _build_context_and_llm()
        from app.agents.vendor_intelligence_agent import VendorIntelligenceAgent

        vendor_id = str(payload.get("vendor_id", "") or "").strip()
        if vendor_id:
            payload = {
                **payload,
                "vendor_records": cache.get_vendor_records(vendor_id),
                "vendor_paths": cache.get_vendor_paths(vendor_id),
            }
        agent = VendorIntelligenceAgent(llm, process_context)
        result = agent.process(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Vendor intelligence execution failed: {str(e)}",
        )


@router.post("/agents/write-prompts")
def write_prompts(payload: dict = Body(...)):
    try:
        process_context, llm = _build_context_and_llm()
        from app.agents.prompt_writer_agent import PromptWriterAgent

        agent = PromptWriterAgent(llm, process_context)
        result = agent.process(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Prompt writer execution failed: {str(e)}",
        )


@router.post("/agents/automation-policy")
def automation_policy(payload: dict = Body(...)):
    try:
        process_context, llm = _build_context_and_llm()
        from app.agents.automation_policy_agent import AutomationPolicyAgent

        agent = AutomationPolicyAgent(llm, process_context)
        result = agent.process(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Automation policy execution failed: {str(e)}",
        )


@router.post("/agents/execute-exception")
def execute_exception(payload: dict = Body(...)):
    try:
        process_context, llm = _build_context_and_llm()
        from app.agents.exception_agent import ExceptionAgent

        agent = ExceptionAgent(llm, process_context)
        result = agent.process(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Exception agent execution failed: {str(e)}",
        )


@router.post("/agents/human-review")
def human_review(payload: dict = Body(...)):
    try:
        process_context, llm = _build_context_and_llm()
        from app.agents.human_in_loop_agent import HumanInLoopAgent

        agent = HumanInLoopAgent(llm, process_context)
        result = agent.process(payload)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Human-in-loop execution failed: {str(e)}",
        )
