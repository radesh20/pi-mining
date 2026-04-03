from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class APIResponse(BaseModel):
    success: bool = True
    data: Any = None
    error: Optional[str] = None


class ChatResponse(BaseModel):
    success: bool = True
    reply: str
    suggested_questions: List[str] = []
    data_sources: List[str] = []
    next_steps: List[str] = []
    context_used: Dict[str, Any] = {}
    scope_label: str = ""
    agent_used: str = ""          # which agent answered this
    error: Optional[str] = None