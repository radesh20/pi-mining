"""
PI-specific agents: RiskAgent, PIExceptionAgent, PIVendorAgent, ActionAgent.

These agents use real process intelligence signals (not LLM calls) to produce
structured, contextual outputs that drive proactive decision-making.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# RiskAgent
# ---------------------------------------------------------------------------

class RiskAgent:
    """
    Evaluates breach probability and timing data against historical process
    performance to determine risk level.
    """

    RISK_HIGH_THRESHOLD = 0.75
    RISK_MEDIUM_THRESHOLD = 0.4

    def run(
        self,
        signals: Dict[str, Any],
        case_data: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        breach_probability: float = float(signals.get("breach_probability") or 0.0)
        dwell_anomaly: float = float(signals.get("dwell_anomaly") or 0.0)
        avg_e2e: float = float(process_context.get("avg_end_to_end_days") or 0.0)
        duration: float = float(case_data.get("duration_days") or 0.0)
        days_until_due = case_data.get("days_until_due")
        vendor_id: str = str(case_data.get("vendor_id") or "UNKNOWN")

        if breach_probability >= self.RISK_HIGH_THRESHOLD:
            risk_level = "HIGH"
        elif breach_probability >= self.RISK_MEDIUM_THRESHOLD:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        # Escalate to HIGH if dwell anomaly is severe even if breach_probability is moderate
        if dwell_anomaly >= 2.0 and risk_level == "MEDIUM":
            risk_level = "HIGH"

        reasoning_parts: list[str] = [
            f"Risk assessment is based on historical process timing, not just due date.",
            f"Breach probability: {round(breach_probability * 100, 1)}% derived from "
            f"remaining time vs historical avg processing time of {round(avg_e2e, 1)} days.",
        ]

        if days_until_due is not None:
            remaining = float(days_until_due)
            reasoning_parts.append(
                f"This case has {round(remaining, 1)} days remaining "
                f"while historical processing takes {round(avg_e2e, 1)} days on average."
            )

        if dwell_anomaly >= 1.5:
            reasoning_parts.append(
                f"Current duration ({round(duration, 1)} days) is "
                f"{round(dwell_anomaly, 2)}x the historical average — indicating process delay."
            )

        if risk_level == "HIGH":
            reasoning_parts.append(
                f"HIGH risk: immediate intervention required to prevent SLA breach."
            )

        return {
            "agent": "RiskAgent",
            "risk_level": risk_level,
            "breach_probability": breach_probability,
            "avg_historical_processing_days": round(avg_e2e, 2),
            "current_duration_days": round(duration, 2),
            "days_until_due": round(float(days_until_due), 2) if days_until_due is not None else None,
            "dwell_anomaly_ratio": round(dwell_anomaly, 3),
            "vendor_id": vendor_id,
            "reasoning": " ".join(reasoning_parts),
        }


# ---------------------------------------------------------------------------
# PIExceptionAgent
# ---------------------------------------------------------------------------

class PIExceptionAgent:
    """
    Analyzes variant risk and dwell anomaly to determine process-path root cause
    and why the current variant leads to exceptions.
    """

    def run(
        self,
        signals: Dict[str, Any],
        case_data: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        variant_risk: str = str(signals.get("variant_risk") or "LOW")
        dwell_anomaly: float = float(signals.get("dwell_anomaly") or 0.0)
        trace_text: str = str(case_data.get("activity_trace_text") or "")
        golden_path: str = str(process_context.get("golden_path") or "")
        exception_rate: float = float(process_context.get("exception_rate") or 0.0)
        avg_e2e: float = float(process_context.get("avg_end_to_end_days") or 0.0)
        duration: float = float(case_data.get("duration_days") or 0.0)

        # Identify root cause from trace deviation
        exception_keywords_map = {
            "exception": "Invoice exception detected in process path",
            "block": "Invoice or PO block encountered in process path",
            "due date passed": "Case has passed its due date in the process trail",
            "moved out": "Invoice was moved out of processing queue",
            "reject": "Rejection step identified in variant path",
            "error": "Error activity detected in process execution",
        }

        trace_lower = trace_text.lower()
        root_causes: list[str] = [
            desc for kw, desc in exception_keywords_map.items() if kw in trace_lower
        ]

        if not root_causes:
            if variant_risk == "HIGH":
                root_causes = ["Case follows a high-frequency exception variant path"]
            elif variant_risk == "MEDIUM":
                root_causes = ["Case deviates from golden path; may encounter exceptions"]
            else:
                root_causes = ["Case follows a low-risk variant path"]

        # Process-path explanation
        deviation_explanation = ""
        if golden_path and trace_text and golden_path.lower() != trace_text.lower():
            deviation_explanation = (
                f"Case process path: '{trace_text}' deviates from the golden path "
                f"'{golden_path}'. This variant has historically led to longer processing "
                f"times and a higher exception rate ({round(exception_rate, 1)}% of cases)."
            )
        elif not golden_path:
            deviation_explanation = (
                f"No golden path reference available; variant risk assessed as {variant_risk} "
                f"based on exception rate ({round(exception_rate, 1)}%)."
            )
        else:
            deviation_explanation = "Case follows the expected golden process path."

        dwell_note = ""
        if dwell_anomaly >= 1.5:
            dwell_note = (
                f"Dwell anomaly ({round(dwell_anomaly, 2)}x): current duration "
                f"{round(duration, 1)} days vs historical avg {round(avg_e2e, 1)} days. "
                f"This variant is stalling at an intermediate process step."
            )

        return {
            "agent": "PIExceptionAgent",
            "variant_risk": variant_risk,
            "dwell_anomaly": round(dwell_anomaly, 3),
            "root_causes": root_causes,
            "process_path": trace_text,
            "golden_path": golden_path,
            "deviation_explanation": deviation_explanation,
            "dwell_note": dwell_note,
            "exception_rate_pct": round(exception_rate, 2),
        }


# ---------------------------------------------------------------------------
# PIVendorAgent
# ---------------------------------------------------------------------------

class PIVendorAgent:
    """
    Provides vendor behavioral insight based on historical trend data.
    """

    def run(
        self,
        signals: Dict[str, Any],
        case_data: Dict[str, Any],
        vendor_stats: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        vendor_trend: str = str(signals.get("vendor_trend") or "STABLE")
        vendor_id: str = str(case_data.get("vendor_id") or "UNKNOWN").strip().upper()

        vendor_record: Dict[str, Any] = {}
        for vs in vendor_stats:
            if str(vs.get("vendor_id") or "").strip().upper() == vendor_id:
                vendor_record = vs
                break

        avg_dpo: float = float(vendor_record.get("avg_dpo") or 0.0)
        exception_rate: float = float(vendor_record.get("exception_rate") or 0.0)
        risk_score: str = str(vendor_record.get("risk_score") or "UNKNOWN")
        invoice_count: int = int(vendor_record.get("invoice_count") or 0)

        if vendor_trend == "DETERIORATING":
            insight = (
                f"Vendor {vendor_id} is DETERIORATING based on historical data: "
                f"exception rate {round(exception_rate, 1)}%, avg DPO {round(avg_dpo, 1)} days, "
                f"risk score {risk_score}. "
                f"Historical trend shows increasing processing delays and exception frequency."
            )
            recommendation = (
                "Escalate to vendor immediately. Proactive outreach needed to resolve "
                "outstanding invoice issues before due date breach."
            )
        else:
            insight = (
                f"Vendor {vendor_id} is STABLE based on historical data: "
                f"exception rate {round(exception_rate, 1)}%, avg DPO {round(avg_dpo, 1)} days, "
                f"risk score {risk_score}."
            )
            recommendation = (
                "Monitor vendor performance. No immediate escalation required "
                "based on historical trend data."
            )

        return {
            "agent": "PIVendorAgent",
            "vendor_id": vendor_id,
            "vendor_trend": vendor_trend,
            "historical_exception_rate_pct": round(exception_rate, 2),
            "historical_avg_dpo_days": round(avg_dpo, 2),
            "historical_risk_score": risk_score,
            "historical_invoice_count": invoice_count,
            "insight": insight,
            "recommendation": recommendation,
        }


# ---------------------------------------------------------------------------
# ActionAgent
# ---------------------------------------------------------------------------

class ActionAgent:
    """
    Synthesizes outputs from RiskAgent, PIExceptionAgent, PIVendorAgent and
    process signals to recommend a clear, feasible next action.
    """

    def run(
        self,
        signals: Dict[str, Any],
        risk_output: Optional[Dict[str, Any]],
        exception_output: Optional[Dict[str, Any]],
        vendor_output: Optional[Dict[str, Any]],
        case_data: Dict[str, Any],
        process_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        risk_level: str = (risk_output or {}).get("risk_level", "LOW")
        variant_risk: str = signals.get("variant_risk", "LOW")
        vendor_trend: str = signals.get("vendor_trend", "STABLE")
        breach_probability: float = float(signals.get("breach_probability") or 0.0)
        dwell_anomaly: float = float(signals.get("dwell_anomaly") or 0.0)

        avg_e2e: float = float(process_context.get("avg_end_to_end_days") or 0.0)
        days_until_due = case_data.get("days_until_due")
        remaining: Optional[float] = float(days_until_due) if days_until_due is not None else None

        # Determine recommended action based on combined signals
        recommended_action, action_reason = self._determine_action(
            risk_level, variant_risk, vendor_trend, breach_probability, dwell_anomaly
        )

        # Assess feasibility based on timing
        feasibility, feasibility_reason = self._assess_feasibility(
            remaining, avg_e2e, risk_level, recommended_action
        )

        # Build consolidated reason from all agent outputs
        reason_parts: list[str] = [action_reason]

        if risk_output:
            reason_parts.append(
                f"Risk: {risk_output.get('risk_level')} "
                f"(breach probability {round(breach_probability * 100, 1)}% based on process timing)."
            )

        if exception_output:
            root_causes = exception_output.get("root_causes", [])
            if root_causes:
                reason_parts.append(f"Exception: {'; '.join(root_causes[:2])}.")

        if vendor_output and vendor_output.get("vendor_trend") == "DETERIORATING":
            reason_parts.append(
                f"Vendor {vendor_output.get('vendor_id')} trend: DETERIORATING "
                f"(exception rate {vendor_output.get('historical_exception_rate_pct')}%)."
            )

        reason_parts.append(feasibility_reason)

        if risk_level == "HIGH" or vendor_trend == "DETERIORATING":
            urgency = "HIGH"
        elif risk_level == "MEDIUM":
            urgency = "MEDIUM"
        else:
            urgency = "LOW"

        return {
            "agent": "ActionAgent",
            "recommended_action": recommended_action,
            "feasibility": feasibility,
            "reason": " ".join(reason_parts),
            "urgency": urgency,
            "signals_evaluated": {
                "breach_probability": breach_probability,
                "variant_risk": variant_risk,
                "vendor_trend": vendor_trend,
                "dwell_anomaly": dwell_anomaly,
                "risk_level": risk_level,
            },
        }

    def _determine_action(
        self,
        risk_level: str,
        variant_risk: str,
        vendor_trend: str,
        breach_probability: float,
        dwell_anomaly: float,
    ) -> tuple[str, str]:
        # Priority order: vendor escalation > exception handling > risk mitigation > monitoring
        if vendor_trend == "DETERIORATING" and risk_level == "HIGH":
            return (
                "Escalate to vendor immediately",
                "Both vendor trend is DETERIORATING and risk is HIGH — "
                "vendor escalation is the most impactful immediate action.",
            )

        if vendor_trend == "DETERIORATING" and variant_risk == "HIGH":
            return (
                "Escalate to vendor immediately and trigger manual invoice review",
                "Vendor is DETERIORATING and case is on a HIGH-risk variant — "
                "dual escalation required.",
            )

        if variant_risk == "HIGH" and risk_level == "HIGH":
            return (
                "Trigger manual invoice review",
                "Case is on a HIGH-risk process variant with HIGH breach probability — "
                "manual review required to prevent exception escalation.",
            )

        if risk_level == "HIGH" and breach_probability >= 0.75:
            return (
                "Prioritize payment execution now",
                "Breach probability exceeds 75% based on historical process timing — "
                "immediate payment prioritization required.",
            )

        if dwell_anomaly >= 2.0:
            return (
                "Reprocess invoice matching step",
                f"Dwell anomaly of {round(dwell_anomaly, 2)}x detected — "
                "case is stalled; reprocessing the matching step will unblock it.",
            )

        if variant_risk == "HIGH":
            return (
                "Trigger manual invoice review",
                "Case follows a HIGH-risk process variant — "
                "manual review prevents common variant failure patterns.",
            )

        if vendor_trend == "DETERIORATING":
            return (
                "Escalate to vendor immediately",
                "Vendor trend is DETERIORATING based on historical data — "
                "proactive escalation prevents SLA breach.",
            )

        if risk_level == "MEDIUM":
            return (
                "Monitor and prepare contingency escalation",
                "MEDIUM risk level — monitor closely and prepare escalation path "
                "if breach probability increases.",
            )

        return (
            "Continue standard processing",
            "All PI signals are within normal bounds — standard processing is appropriate.",
        )

    def _assess_feasibility(
        self,
        remaining: Optional[float],
        avg_e2e: float,
        risk_level: str,
        recommended_action: str,
    ) -> tuple[str, str]:
        # Estimate how long the recommended action takes
        action_time_map = {
            "Escalate to vendor immediately": 0.5,
            "Trigger manual invoice review": 1.0,
            "Prioritize payment execution now": 0.25,
            "Reprocess invoice matching step": 1.5,
            "Monitor and prepare contingency escalation": 0.0,
            "Continue standard processing": 0.0,
        }
        action_time = 1.0  # default
        recommended_action_lower = recommended_action.lower()
        for key, days in action_time_map.items():
            if key.lower() in recommended_action_lower:
                action_time = days
                break

        if remaining is None:
            return (
                "Feasible",
                f"No due date constraint found; action '{recommended_action}' can proceed.",
            )

        if remaining <= 0:
            return (
                "Not feasible",
                f"Due date has already passed ({round(remaining, 1)} days overdue). "
                f"Action is post-breach; focus on damage mitigation.",
            )

        if remaining >= action_time:
            return (
                "Feasible",
                f"Sufficient time remaining ({round(remaining, 1)} days) "
                f"to complete '{recommended_action}' (est. {action_time} days).",
            )

        return (
            "Not feasible",
            f"Insufficient time: {round(remaining, 1)} days remaining, "
            f"but '{recommended_action}' requires ~{action_time} days. "
            f"Escalate immediately to minimize breach impact.",
        )
