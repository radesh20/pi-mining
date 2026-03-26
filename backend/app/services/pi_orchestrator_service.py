"""
PI Orchestrator Service.

Dynamically selects and coordinates PI agents based on real process signals.
Contrasts BI decision (days-remaining only) with PI decision (process signals).

Flow: signals → select agents → Risk → Exception → Vendor → Action
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.services.pi_signal_service import extract_signals
from app.agents.pi_agents import (
    RiskAgent,
    PIExceptionAgent,
    PIVendorAgent,
    ActionAgent,
)

logger = logging.getLogger(__name__)

# Dynamic selection thresholds
_BREACH_PROB_THRESHOLD = 0.6
_DWELL_ANOMALY_THRESHOLD = 1.5


class PIOrchestrator:
    """
    PI-driven orchestrator.

    1. Fetch real process signals from existing services.
    2. Dynamically select agents based on signals.
    3. Run selected agents with shared context.
    4. Return BI vs PI comparison.
    """

    def __init__(
        self,
        case_data: Dict[str, Any],
        process_context: Dict[str, Any],
        vendor_stats: List[Dict[str, Any]],
    ) -> None:
        self.case_data = case_data
        self.process_context = process_context
        self.vendor_stats = vendor_stats

    def run(self) -> Dict[str, Any]:
        # ── Step 1: Extract PI signals ────────────────────────────────────
        signals = extract_signals(self.case_data, self.process_context, self.vendor_stats)
        logger.info("PI signals extracted: %s", signals)

        # ── Step 2: BI decision (naive, days-remaining only) ──────────────
        bi_decision = self._bi_decision()

        # ── Step 3: Dynamic agent selection ───────────────────────────────
        selected: list[str] = []

        if float(signals.get("breach_probability") or 0.0) > _BREACH_PROB_THRESHOLD:
            selected.append("RiskAgent")

        if signals.get("variant_risk") == "HIGH":
            selected.append("PIExceptionAgent")

        if signals.get("vendor_trend") == "DETERIORATING":
            selected.append("PIVendorAgent")

        # Dwell anomaly can independently trigger RiskAgent when breach probability
        # hasn't already triggered it
        if float(signals.get("dwell_anomaly") or 0.0) >= _DWELL_ANOMALY_THRESHOLD:
            if "RiskAgent" not in selected:
                selected.append("RiskAgent")

        # ActionAgent is always last if any other agent triggered
        action_triggered = len(selected) > 0
        if action_triggered:
            selected.append("ActionAgent")

        # ── Step 4: Execute selected agents ───────────────────────────────
        risk_output: Optional[Dict[str, Any]] = None
        exception_output: Optional[Dict[str, Any]] = None
        vendor_output: Optional[Dict[str, Any]] = None
        action_output: Optional[Dict[str, Any]] = None
        agent_outputs: list[Dict[str, Any]] = []

        if "RiskAgent" in selected:
            risk_output = RiskAgent().run(signals, self.case_data, self.process_context)
            agent_outputs.append(risk_output)

        if "PIExceptionAgent" in selected:
            exception_output = PIExceptionAgent().run(signals, self.case_data, self.process_context)
            agent_outputs.append(exception_output)

        if "PIVendorAgent" in selected:
            vendor_output = PIVendorAgent().run(signals, self.case_data, self.vendor_stats)
            agent_outputs.append(vendor_output)

        if "ActionAgent" in selected:
            action_output = ActionAgent().run(
                signals,
                risk_output,
                exception_output,
                vendor_output,
                self.case_data,
                self.process_context,
            )
            agent_outputs.append(action_output)

        # ── Step 5: PI decision ────────────────────────────────────────────
        pi_decision = self._pi_decision(signals, risk_output, action_output)

        # ── Step 6: Build flow string ──────────────────────────────────────
        flow = " → ".join(selected) if selected else "No agents triggered"

        return {
            "bi_decision": bi_decision,
            "pi_decision": pi_decision,
            "agents_triggered": selected,
            "agent_outputs": agent_outputs,
            "flow": flow,
            "final_action": action_output.get("recommended_action") if action_output else "No action required",
            "reason": action_output.get("reason") if action_output else signals.get("pi_reason", ""),
            "signals": signals,
        }

    # ------------------------------------------------------------------
    # BI decision: based ONLY on days remaining to due date
    # ------------------------------------------------------------------
    def _bi_decision(self) -> str:
        days_until_due = self.case_data.get("days_until_due")
        if days_until_due is None:
            return "BI: Insufficient data — no due date available. Risk cannot be assessed."

        remaining = float(days_until_due)
        if remaining <= 0:
            return "BI: Past due — high risk."
        if remaining <= 7:
            return f"BI: {round(remaining, 1)} days remaining — MEDIUM risk."
        return f"BI: {round(remaining, 1)} days remaining — LOW risk."

    # ------------------------------------------------------------------
    # PI decision: based on real process signals
    # ------------------------------------------------------------------
    def _pi_decision(
        self,
        signals: Dict[str, Any],
        risk_output: Optional[Dict[str, Any]],
        action_output: Optional[Dict[str, Any]],
    ) -> str:
        risk_level = (risk_output or {}).get("risk_level", "LOW")
        breach_pct = round(float(signals.get("breach_probability") or 0.0) * 100, 1)
        variant_risk = signals.get("variant_risk", "LOW")
        vendor_trend = signals.get("vendor_trend", "STABLE")

        parts = [
            f"PI: {risk_level} risk based on process intelligence.",
            f"Breach probability {breach_pct}% (from historical timing data).",
            f"Variant risk: {variant_risk}.",
            f"Vendor trend: {vendor_trend}.",
        ]
        if action_output:
            parts.append(f"Recommended: {action_output.get('recommended_action')}.")
        return " ".join(parts)
