"""
GuardrailViolation and GuardrailResult — 
the two core types used across all guardrail checks.
"""
from dataclasses import dataclass


class GuardrailViolation(Exception):
    """
    Raised when an agent output violates a hard guardrail rule.
    Caught by the orchestrator to trigger escalation or blocking.
    """
    def __init__(self, rule_id: str, reason: str):
        self.rule_id = rule_id
        self.reason = reason
        super().__init__(f"[{rule_id}] {reason}")


@dataclass
class GuardrailResult:
    """
    Returned from guardrail checks that do not raise but instead 
    override or modify the agent's output.
    """
    passed: bool
    rule_id: str
    reason: str
    action_taken: str  # e.g. "OVERRIDDEN_TO_HUMAN_REQUIRED", "BLOCKED", "ALLOWED"
