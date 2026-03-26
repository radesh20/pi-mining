"""
Process Intelligence Signal Layer.

Extracts structured PI signals from real Celonis/cache data:
  - breach_probability : float [0, 1]
  - variant_risk       : LOW | MEDIUM | HIGH
  - dwell_anomaly      : float  (ratio of current duration to historical median)
  - vendor_trend       : STABLE | DETERIORATING
  - pi_reason          : str explaining the decision
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Thresholds
_BREACH_HIGH_THRESHOLD = 0.6
_DWELL_ANOMALY_HIGH_THRESHOLD = 1.5
_EXCEPTION_RATE_DETERIORATING = 40.0
_RISK_SCORE_DETERIORATING = {"HIGH", "CRITICAL"}


def extract_signals(
    case_data: Dict[str, Any],
    process_context: Dict[str, Any],
    vendor_stats: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Derive PI signals from real Celonis data.

    Parameters
    ----------
    case_data       : dict returned by DataCacheService.get_invoice_case()
    process_context : dict returned by DataCacheService.get_process_context()
    vendor_stats    : list returned by DataCacheService.get_vendor_stats()

    Returns
    -------
    dict with keys: breach_probability, variant_risk, dwell_anomaly,
                    vendor_trend, pi_reason
    """
    breach_probability = _compute_breach_probability(case_data, process_context)
    variant_risk = _compute_variant_risk(case_data, process_context)
    dwell_anomaly = _compute_dwell_anomaly(case_data, process_context)
    vendor_trend = _compute_vendor_trend(case_data, vendor_stats)
    pi_reason = _build_pi_reason(
        case_data,
        process_context,
        breach_probability,
        variant_risk,
        dwell_anomaly,
        vendor_trend,
    )

    return {
        "breach_probability": round(breach_probability, 4),
        "variant_risk": variant_risk,
        "dwell_anomaly": round(dwell_anomaly, 4),
        "vendor_trend": vendor_trend,
        "pi_reason": pi_reason,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_breach_probability(
    case_data: Dict[str, Any],
    process_context: Dict[str, Any],
) -> float:
    """
    Estimate probability that the case will miss its due date based on
    historical average processing time vs remaining time to due date.

    Formula:
        remaining = days_until_due (from case)
        historical = avg_end_to_end_days (from process_context)
        If remaining <= 0            → 1.0 (already breached)
        If remaining >= 2*historical → 0.0 (lots of headroom)
        Else                         → 1 - (remaining / historical)
    """
    avg_e2e: float = float(process_context.get("avg_end_to_end_days") or 0.0)
    days_until_due = case_data.get("days_until_due")

    if days_until_due is None:
        # No due date — use duration vs historical avg
        duration: float = float(case_data.get("duration_days") or 0.0)
        if avg_e2e <= 0:
            return 0.0
        ratio = duration / avg_e2e
        return round(min(1.0, max(0.0, ratio - 1.0)), 4)

    remaining: float = float(days_until_due)
    if remaining <= 0:
        return 1.0

    if avg_e2e <= 0:
        return 0.0

    # Normalize: 0 remaining → 1.0, remaining == historical → ~0 (edge)
    prob = 1.0 - (remaining / avg_e2e)
    return round(min(1.0, max(0.0, prob)), 4)


def _compute_variant_risk(
    case_data: Dict[str, Any],
    process_context: Dict[str, Any],
) -> str:
    """
    Classify variant risk by comparing the case process path to the golden path
    and known exception patterns.

    HIGH   – case is on a known exception variant
    MEDIUM – case is off the golden path but not a known exception
    LOW    – case follows the golden path
    """
    trace_text: str = str(case_data.get("activity_trace_text") or "").lower()
    golden_path: str = str(process_context.get("golden_path") or "").lower()
    exception_rate: float = float(process_context.get("exception_rate") or 0.0)

    # Check for known exception keywords in the activity trace
    exception_keywords = ["exception", "block", "due date passed", "moved out", "error", "reject"]
    has_exception_activity = any(kw in trace_text for kw in exception_keywords)

    if has_exception_activity:
        return "HIGH"

    if not golden_path or not trace_text:
        # Cannot compare — use exception rate as proxy
        if exception_rate >= 40:
            return "HIGH"
        if exception_rate >= 20:
            return "MEDIUM"
        return "LOW"

    # Compare overlap with golden path tokens
    golden_steps = set(golden_path.split(" → "))
    trace_steps = set(trace_text.split(" → "))

    overlap = len(golden_steps & trace_steps)
    total = max(len(golden_steps), 1)
    conformance = overlap / total

    if conformance >= 0.8:
        return "LOW"
    if conformance >= 0.5:
        return "MEDIUM"
    return "HIGH"


def _compute_dwell_anomaly(
    case_data: Dict[str, Any],
    process_context: Dict[str, Any],
) -> float:
    """
    Ratio of the case's current processing duration to the historical
    average end-to-end duration.

    > 1.5 → anomalously long (high dwell)
    ~1.0  → normal
    < 1.0 → faster than average
    """
    avg_e2e: float = float(process_context.get("avg_end_to_end_days") or 0.0)
    duration: float = float(case_data.get("duration_days") or 0.0)

    if avg_e2e <= 0:
        return 1.0

    return round(duration / avg_e2e, 4)


def _compute_vendor_trend(
    case_data: Dict[str, Any],
    vendor_stats: List[Dict[str, Any]],
) -> str:
    """
    Determine vendor trend based on historical vendor performance.

    DETERIORATING if:
      - vendor risk_score is HIGH or CRITICAL
      - OR vendor exception_rate >= threshold

    STABLE otherwise.
    """
    vendor_id: str = str(case_data.get("vendor_id") or "").strip().upper()
    if not vendor_id or vendor_id == "UNKNOWN":
        return "STABLE"

    vendor_record: Dict[str, Any] = {}
    for vs in vendor_stats:
        if str(vs.get("vendor_id") or "").strip().upper() == vendor_id:
            vendor_record = vs
            break

    if not vendor_record:
        return "STABLE"

    risk_score: str = str(vendor_record.get("risk_score") or "").upper()
    exception_rate: float = float(vendor_record.get("exception_rate") or 0.0)
    avg_dpo: float = float(vendor_record.get("avg_dpo") or 0.0)

    if risk_score in _RISK_SCORE_DETERIORATING:
        return "DETERIORATING"
    if exception_rate >= _EXCEPTION_RATE_DETERIORATING:
        return "DETERIORATING"
    if avg_dpo >= 60:
        return "DETERIORATING"

    return "STABLE"


def _build_pi_reason(
    case_data: Dict[str, Any],
    process_context: Dict[str, Any],
    breach_probability: float,
    variant_risk: str,
    dwell_anomaly: float,
    vendor_trend: str,
) -> str:
    parts: list[str] = []

    avg_e2e = float(process_context.get("avg_end_to_end_days") or 0.0)
    duration = float(case_data.get("duration_days") or 0.0)
    days_until_due = case_data.get("days_until_due")
    vendor_id = str(case_data.get("vendor_id") or "UNKNOWN")

    if days_until_due is not None:
        parts.append(
            f"Case has {round(float(days_until_due), 1)} days remaining to due date; "
            f"historical average processing time is {round(avg_e2e, 1)} days."
        )

    if breach_probability >= _BREACH_HIGH_THRESHOLD:
        parts.append(
            f"Breach probability is {round(breach_probability * 100, 1)}% — "
            f"based on process history, not just the calendar due date."
        )

    if variant_risk == "HIGH":
        golden = process_context.get("golden_path", "")
        trace = case_data.get("activity_trace_text", "")
        parts.append(
            f"Case is on a HIGH-risk process variant (trace: '{trace}'), "
            f"diverging from golden path ('{golden}')."
        )
    elif variant_risk == "MEDIUM":
        parts.append("Case is partially off the golden process path.")

    if dwell_anomaly >= _DWELL_ANOMALY_HIGH_THRESHOLD:
        parts.append(
            f"Dwell anomaly detected: current duration {round(duration, 1)} days "
            f"vs historical avg {round(avg_e2e, 1)} days "
            f"(ratio {round(dwell_anomaly, 2)}x)."
        )

    if vendor_trend == "DETERIORATING":
        parts.append(
            f"Vendor {vendor_id} shows DETERIORATING trend based on "
            f"historical exception rate and DPO patterns."
        )
    else:
        parts.append(f"Vendor {vendor_id} is STABLE based on historical performance data.")

    return " ".join(parts) if parts else "PI signals computed from real Celonis process data."
