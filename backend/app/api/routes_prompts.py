from fastapi import APIRouter, Query, HTTPException
from app.services.celonis_service import CelonisConnectionError
from app.services.azure_openai_service import AzureOpenAIService
from app.services.prompt_generation_service import PromptGenerationService
from app.services.data_cache_service import get_data_cache_service

router = APIRouter()


@router.get("/prompts/deep-dive")
def get_agent_deep_dive(
    agent_name: str = Query(default="Invoice Exception Agent"),
):
    """Deliverable 2: Generate process-aware prompts for an agent."""
    try:
        cache = get_data_cache_service()
        llm = AzureOpenAIService()
        prompt_service = PromptGenerationService(llm)

        context = cache.get_process_context()
        case_data = cache.get_representative_exception_case()
        exception_records = cache.get_all_exception_records()
        result = prompt_service.generate_deep_dive(
            agent_name,
            context,
            case_data=case_data,
            exception_records=exception_records,
        )
        return {"success": True, "data": result}
    except (CelonisConnectionError, ValueError) as e:

        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/comparison")
def get_prompt_comparison(
    agent_name: str = Query(default="Invoice Exception Agent"),
):
    """Deliverable 2: Compare prompts with vs without process mining."""
    try:
        cache = get_data_cache_service()
        llm = AzureOpenAIService()
        prompt_service = PromptGenerationService(llm)

        context = cache.get_process_context()
        result = prompt_service.generate_comparison(agent_name, context)
        return {"success": True, "data": result}
    except (CelonisConnectionError, ValueError) as e:

        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
