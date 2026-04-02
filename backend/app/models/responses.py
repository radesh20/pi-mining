from pydantic import BaseModel
from typing import Any, Dict, Optional


class APIResponse(BaseModel):
    success: bool = True
    data: Any = None
    error: Optional[str] = None


class ChatResponse(BaseModel):
    success: bool = True
    reply: str
    context_used: Dict[str, Any] = {}
    error: Optional[str] = None