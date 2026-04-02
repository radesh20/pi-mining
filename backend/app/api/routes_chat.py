"""
app/api/routes_chat.py

Chatbot endpoint — POST /chat
Wires ChatRequest → ChatService → ChatResponse.
"""

import logging
from fastapi import APIRouter, HTTPException

from app.models.requests import ChatRequest
from app.models.responses import ChatResponse, APIResponse
from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.agent_recommendation_service import AgentRecommendationService
from app.services.chat_service import ChatService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"])


def _get_chat_service() -> ChatService:
    """
    Instantiate all dependencies.
    Uses the same pattern as your other routes — direct instantiation.
    If you later add FastAPI dependency injection, swap this out.
    """
    llm = AzureOpenAIService()
    celonis = CelonisService()
    process_insight = ProcessInsightService(celonis_service=celonis)
    agent_rec = AgentRecommendationService(llm=llm)
    return ChatService(
        llm=llm,
        celonis=celonis,
        process_insight=process_insight,
        agent_recommendation=agent_rec,
    )


@router.post("/", response_model=ChatResponse)
def chat(req: ChatRequest):
    """
    Process-aware chatbot endpoint.

    Accepts:
    - message            : the user's question
    - case_id (optional) : scopes context to a specific invoice case
    - vendor_id (optional): scopes context to a specific vendor
    - conversation_history: list of {role, content} dicts for multi-turn support

    Returns:
    - reply       : GPT-4o answer grounded in Celonis PI data
    - context_used: what PI data was injected (useful for frontend debugging)
    """
    try:
        service = _get_chat_service()

        # Convert Pydantic ChatMessage objects to plain dicts for the service layer
        history = [
            {"role": msg.role, "content": msg.content}
            for msg in req.conversation_history
        ]

        result = service.chat(
            message=req.message,
            conversation_history=history,
            case_id=req.case_id,
            vendor_id=req.vendor_id,
        )

        return ChatResponse(
            success=result["success"],
            reply=result["reply"],
            context_used=result.get("context_used", {}),
            error=result.get("error"),
        )

    except Exception as exc:
        logger.error("Chat endpoint error: %s", str(exc), exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/health")
def chat_health():
    """Quick liveness check for the chat service."""
    return APIResponse(success=True, data={"status": "chat service is up"})