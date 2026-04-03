"""
Guardrail layer for the P2P AI agent system.
All guardrail violations, results, and the exception registry live here.
"""
from .exceptions import GuardrailViolation, GuardrailResult
from .exception_registry import classify_exception, get_handler, EXCEPTION_REGISTRY

__all__ = [
    "GuardrailViolation",
    "GuardrailResult", 
    "classify_exception",
    "get_handler",
    "EXCEPTION_REGISTRY"
]
