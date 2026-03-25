from pydantic import BaseModel
from typing import Any, Optional


class APIResponse(BaseModel):
    success: bool = True
    data: Any = None
    error: Optional[str] = None