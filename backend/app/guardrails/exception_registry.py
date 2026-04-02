"""
Exception registry — replaces all inline keyword if/else blocks in orchestrator_service.py.
Add new exception types here. Do not add them to the orchestrator.
"""
import re

EXCEPTION_REGISTRY = {
    "payment_terms_mismatch": {
        "handler_agent": "exception_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": True,
        "confidence_threshold": 0.80,
        "keywords": ["payment terms mismatch", "payment_terms_mismatch", "payment terms"]
    },
    "tax_mismatch": {
        "handler_agent": "exception_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": False,
        "confidence_threshold": 0.90,
        "keywords": ["tax mismatch", "tax code", "tax correction"]
    },
    "early_payment": {
        "handler_agent": "automation_policy_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": True,
        "confidence_threshold": 0.75,
        "keywords": ["early payment", "early_payment"]
    },
    "short_payment_terms": {
        "handler_agent": "exception_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": True,
        "confidence_threshold": 0.80,
        "keywords": ["short payment terms", "0-day", "short terms"]
    },
    "paid_late": {
        "handler_agent": "exception_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": False,
        "confidence_threshold": 0.85,
        "keywords": ["paid late", "payment overdue", "overdue"]
    },
    "invoice_exception": {
        "handler_agent": "exception_agent",
        "escalation_agent": "human_in_loop_agent",
        "auto_resolvable": True,
        "confidence_threshold": 0.80,
        "keywords": ["invoice exception", "stuck", "80 days"]
    }
}


def classify_exception(scenario_text: str) -> dict:
    """
    Classify a scenario text into a known exception type.
    Replaces the inline keyword if/else in orchestrator_service.py ~lines 1132-1192.
    
    Returns the registry entry dict, or the invoice_exception fallback.
    """
    text = scenario_text.lower()
    ordered = sorted(
        EXCEPTION_REGISTRY.items(),
        key=lambda item: max(len(kw) for kw in item[1]["keywords"]),
        reverse=True,
    )
    for exception_type, config in ordered:
        for kw in config["keywords"]:
            pattern = r"\b" + re.escape(kw.lower()) + r"\b"
            if re.search(pattern, text):
                return {"id": exception_type, **config}
    return {"id": "invoice_exception", **EXCEPTION_REGISTRY["invoice_exception"]}


def get_handler(exception_type: str) -> dict:
    """
    Get the handler config for a known exception type.
    Returns invoice_exception config as fallback.
    """
    return EXCEPTION_REGISTRY.get(exception_type, EXCEPTION_REGISTRY["invoice_exception"])
