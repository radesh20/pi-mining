"""
app/services/chat_service.py

FIXES IN THIS VERSION (on top of previous):
  A. _derive_pi_context_fields() -- NEW function that derives process_step,
     deviation_point, variant_path, cycle_time, vendor_behavior from
     available global/exception/vendor/case data for ANY query type.
     The LLM therefore ALWAYS receives pre-filled values and must never
     write "[Data unavailable]" for these fields.
  B. System prompt PI Context Used block is now PRE-FILLED with real values
     injected as {pi_ctx_*} template vars -- the LLM is instructed to
     REPRODUCE them verbatim, not invent or omit them.
  C. _extract_pi_evidence() populates pi_context_panel dict that the
     frontend PI Context Used panel reads, derived the same way as (A).
  D. All five PI Context Used panel fields guaranteed non-empty for every
     query category: global, exception, bottleneck, vendor, case.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.services.azure_openai_service import AzureOpenAIService
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.suggestion_service import SuggestionService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GRAPH_TRIGGER_KEYWORDS = [
    "graph", "diagram", "flow", "visuali", "map",
    "process path", "show path", "show me path", "show process",
    "draw", "chart", "show flow", "show me the flow",
    "display path", "process flow",
]

EXCEPTION_KEYWORDS = [
    "exception", "due date passed", "block", "blocked", "hold",
    "parked", "park", "reject", "returned", "moved out", "stuck",
    "overdue", "error", "failed", "suspend",
]
_EXCEPTION_PATTERN = re.compile(
    "|".join(re.escape(k) for k in EXCEPTION_KEYWORDS), re.IGNORECASE
)

import json

# ---------------------------------------------------------------------------
# Universal entity ID extractor  (replaces hard-coded vendor/case regex)
# ---------------------------------------------------------------------------

_UNIVERSAL_ID_RE = re.compile(
    # Pattern A: explicit keyword + any alphanumeric ID (invoice 5701, PO-4500, mat 100-1)
    r'\b(?:invoice(?:\s+id)?|case(?:\s+id)?|po|purchase\s+order|material|mat|vendor(?:\s+id)?|lifnr|doc(?:ument)?|supplier(?:\s+id)?)'  
    r'\s*[:\-]?\s*([A-Za-z0-9\-_\.]{3,20})\b'
    # Pattern B: bare long number (≥6 digits) with no preceding keyword
    r'|\b(\d{6,15})\b'
    # Pattern C: alphanumeric token with mix of letters+digits (INV-001, MAT456, PO45000, 4500001234)
    r'|\b([A-Z]{1,5}[-_]?\d{4,15})\b',
    re.IGNORECASE,
)

# Tokens that look like IDs but are generic words — skip these
_SKIP_TOKENS = {
    "THE", "AND", "FOR", "WITH", "THIS", "THAT", "WHAT",
    "WHEN", "SHOULD", "CLOSE", "CASE", "SHOW", "GIVE",
}

def _extract_all_entity_ids(message: str) -> List[str]:
    """
    Extracts every structured ID token from the user message.
    Returns a deduplicated list, normalised to UPPERCASE.
    Covers: numeric IDs, alphanumeric IDs (INV-001, MAT456),
    and IDs preceded by explicit entity-type keywords.
    """
    found: List[str] = []
    seen: set = set()
    for m in _UNIVERSAL_ID_RE.finditer(message):
        val = (m.group(1) or m.group(2) or m.group(3) or "").strip().upper()
        if not val or val in _SKIP_TOKENS or val in seen:
            continue
        # Skip tokens that are only letters (likely a word, not an ID)
        if val.isalpha() and len(val) <= 4:
            continue
        found.append(val)
        seen.add(val)
    return found


def _extract_vendor_id(message: str) -> Optional[str]:
    """Legacy helper — returns the first detected ID that looks vendor-like."""
    _v_re = re.compile(
        r'\b(?:vendor(?:\s+id)?|lifnr|supplier(?:\s+id)?)\s*[:\-]?\s*(\d{5,12})\b',
        re.IGNORECASE,
    )
    m = _v_re.search(message)
    return m.group(1).strip() if m else None


def _extract_case_id(message: str) -> Optional[str]:
    """Legacy helper — kept for backward compatibility."""
    ids = _extract_all_entity_ids(message)
    if not ids:
        return None
    # Prefer ids that look like they're NOT vendor-related
    q = message.lower()
    if "vendor" in q or "supplier" in q or "lifnr" in q:
        return None
    return ids[0]


# ---------------------------------------------------------------------------
# Scope instruction templates
# ---------------------------------------------------------------------------

_VENDOR_SCOPE_INSTRUCTION = """
VENDOR SCOPE ACTIVE: {vendor_id}
Your ENTIRE answer must be about this vendor only.
If the vendor snapshot says NOT FOUND, say so clearly and list which vendor IDs DO exist in the data.
Do not discuss other vendors or global averages except as comparison benchmarks.
"""

_CASE_SCOPE_INSTRUCTION = """
CASE SCOPE ACTIVE: {case_id}
Your ENTIRE answer must be about this specific case only.
If the case was not found, say so clearly and show the sample IDs from the data.
Do not discuss other cases except as similar-case comparisons.
"""

_ENTITY_SCOPE_INSTRUCTION = """
ENTITY SCOPE ACTIVE: {entity_id}  (detected type: {entity_type})
Answer exclusively using the ENTITY DATA block provided above from the Celonis Knowledge Model.
Surface all related entities listed under "related" (e.g. linked POs, vendor, materials).
If fields are missing or null, say so — do not fabricate data.
"""

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _user_wants_graph(message: str) -> bool:
    q = message.lower()
    return any(kw in q for kw in GRAPH_TRIGGER_KEYWORDS)


def _extract_vendor_id_legacy(message: str) -> Optional[str]:
    """Legacy internal alias — prefer _extract_all_entity_ids."""
    return _extract_vendor_id(message)


def _normalise_id(val: Any) -> str:
    return str(val).strip().upper()


def _safe_str_col(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


# ---------------------------------------------------------------------------
# Compute transition durations from raw event log
# ---------------------------------------------------------------------------

def _compute_transition_durations(event_log: pd.DataFrame) -> Dict[str, float]:
    if event_log.empty:
        return {}
    try:
        df = (
            event_log[["case_id", "activity", "timestamp"]]
            .dropna(subset=["timestamp"])
            .sort_values(["case_id", "timestamp"])
            .copy()
        )
        df["next_activity"] = df.groupby("case_id")["activity"].shift(-1)
        df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
        df = df.dropna(subset=["next_activity", "next_ts"])
        df["dur_days"] = (df["next_ts"] - df["timestamp"]).dt.total_seconds() / 86400
        df = df[df["dur_days"] >= 0]
        df["transition"] = (
            _safe_str_col(df["activity"]) + " -> " + _safe_str_col(df["next_activity"])
        )
        result = (
            df.groupby("transition")["dur_days"]
            .mean()
            .round(2)
            .sort_values(ascending=False)
            .to_dict()
        )
        return result
    except Exception as exc:
        logger.warning("_compute_transition_durations failed: %s", exc)
        return {}


def _compute_activity_durations(event_log: pd.DataFrame) -> Dict[str, float]:
    if event_log.empty:
        return {}
    try:
        df = (
            event_log[["case_id", "activity", "timestamp"]]
            .dropna(subset=["timestamp"])
            .sort_values(["case_id", "timestamp"])
            .copy()
        )
        df["next_ts"] = df.groupby("case_id")["timestamp"].shift(-1)
        df = df.dropna(subset=["next_ts"])
        df["dur_days"] = (df["next_ts"] - df["timestamp"]).dt.total_seconds() / 86400
        df = df[df["dur_days"] >= 0]
        result = (
            df.groupby("activity")["dur_days"]
            .mean()
            .round(2)
            .sort_values(ascending=False)
            .to_dict()
        )
        return result
    except Exception as exc:
        logger.warning("_compute_activity_durations failed: %s", exc)
        return {}


def _compute_vendor_summary(event_log: pd.DataFrame, top_n: int = 10) -> List[Dict]:
    if event_log.empty or "vendor_id" not in event_log.columns:
        return []
    try:
        df = event_log.copy()
        df["vendor_id"] = _safe_str_col(df["vendor_id"]).str.strip()
        df = df[df["vendor_id"].str.len() > 0]

        total = df.groupby("vendor_id")["case_id"].nunique().rename("total_cases")

        exc_mask = _safe_str_col(df["activity"]).str.contains(
            _EXCEPTION_PATTERN.pattern, case=False, na=False, regex=True
        )
        exc_cases = (
            df[exc_mask].groupby("vendor_id")["case_id"].nunique().rename("exc_cases")
        )
        summary = pd.concat([total, exc_cases], axis=1).fillna(0)
        summary["exc_rate"] = (summary["exc_cases"] / summary["total_cases"] * 100).round(2)
        summary = summary.sort_values("exc_rate", ascending=False).head(top_n)
        return summary.reset_index().to_dict("records")
    except Exception as exc:
        logger.warning("_compute_vendor_summary failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# NEW: Derive PI Context Used fields for ANY query category
# ---------------------------------------------------------------------------

def _derive_pi_context_fields(
    message: str,
    context_used: Dict[str, Any],
    process_ctx: Optional[Dict],
    event_log: pd.DataFrame,
) -> Dict[str, str]:
    """
    Returns a dict with keys:
        process_step, deviation_point, variant_path,
        cycle_time, vendor_behavior
    guaranteed to be non-empty strings for any query type.
    These are injected into both the system prompt and the frontend panel.
    """
    q = message.lower()
    glob = context_used.get("global", {})
    case_ctx = context_used.get("case", {})
    vendor_ctx = context_used.get("vendor", {})
    exc_cases = context_used.get("exception_cases", [])
    vendor_summary = context_used.get("vendor_summary", [])

    avg_e2e = glob.get("avg_end_to_end_days") or (
        process_ctx.get("avg_end_to_end_days") if process_ctx else None
    )
    bottleneck = glob.get("bottleneck") or (
        process_ctx.get("bottleneck", {}) if process_ctx else {}
    )
    golden_path = (process_ctx or {}).get("golden_path", "")
    golden_pct = (process_ctx or {}).get("golden_path_percentage", 0)
    exc_patterns = glob.get("exception_patterns", [])
    variants = glob.get("variants", [])

    # ── Process step ────────────────────────────────────────────────────────
    if case_ctx.get("current_stage"):
        process_step = case_ctx["current_stage"]
    elif vendor_ctx.get("most_common_variant"):
        # last step in most common variant
        steps = vendor_ctx["most_common_variant"].split(" -> ")
        process_step = steps[-1] if steps else vendor_ctx["most_common_variant"]
    elif bottleneck.get("activity"):
        process_step = f"{bottleneck['activity']} (bottleneck)"
    elif exc_cases:
        # most common current stage among active exceptions
        active = [e for e in exc_cases if e.get("is_active")]
        if active:
            from collections import Counter
            stage_counts = Counter(e["current_stage"] for e in active if e.get("current_stage"))
            if stage_counts:
                process_step = f"{stage_counts.most_common(1)[0][0]} ({stage_counts.most_common(1)[0][1]} active cases)"
            else:
                process_step = active[0].get("exception_type", "Exception stage")
        elif exc_patterns:
            process_step = exc_patterns[0].get("exception_type", "Exception handling")
        else:
            process_step = "Exception handling"
    elif exc_patterns:
        process_step = exc_patterns[0].get("exception_type", "Exception handling")
    elif golden_path:
        steps = golden_path.split(" -> ")
        process_step = steps[-1] if steps else golden_path
    else:
        process_step = "Invoice processing"

    # ── Deviation point ──────────────────────────────────────────────────────
    if case_ctx.get("variant") and golden_path:
        case_steps = case_ctx["variant"].split(" -> ")
        golden_steps = golden_path.split(" -> ")
        deviation_point = "None — on golden path"
        for i, step in enumerate(case_steps):
            if i >= len(golden_steps) or step != golden_steps[i]:
                deviation_point = f"{step} (step {i+1}, expected: {golden_steps[i] if i < len(golden_steps) else 'end'})"
                break
    elif case_ctx.get("is_exception"):
        deviation_point = f"Exception at: {case_ctx.get('current_stage', 'unknown stage')}"
    elif exc_cases:
        active = [e for e in exc_cases if e.get("is_active")]
        if active:
            from collections import Counter
            exc_type_counts = Counter(e["exception_type"] for e in active if e.get("exception_type"))
            if exc_type_counts:
                top_exc, top_cnt = exc_type_counts.most_common(1)[0]
                deviation_point = f"{top_exc} ({top_cnt} active cases)"
            else:
                deviation_point = f"Exception state ({len(active)} active cases)"
        else:
            deviation_point = "None — on golden path"
    elif bottleneck.get("activity") and any(
        kw in q for kw in ["bottleneck", "delay", "slow", "wait", "cycle", "time"]
    ):
        deviation_point = f"Delay at: {bottleneck['activity']} ({bottleneck.get('duration_days', '?')} days)"
    else:
        deviation_point = "None — on golden path"

    # ── Variant path ─────────────────────────────────────────────────────────
    if case_ctx.get("variant"):
        variant_path = case_ctx["variant"]
    elif vendor_ctx.get("most_common_variant"):
        variant_path = vendor_ctx["most_common_variant"]
    elif exc_cases and exc_patterns:
        # Build the most common exception path from patterns
        top_exc = exc_patterns[0].get("exception_type", "Exception")
        variant_path = f"Standard flow → {top_exc}"
    elif variants:
        variant_path = variants[0].get("variant", golden_path or "Standard flow")
    elif golden_path:
        variant_path = golden_path
    else:
        variant_path = "Standard invoice processing flow"

    # Truncate very long paths for display
    if len(variant_path) > 120:
        steps_list = variant_path.split(" -> ")
        if len(steps_list) > 5:
            variant_path = " -> ".join(steps_list[:3]) + f" -> ... ({len(steps_list)-4} steps) -> " + steps_list[-1]

    # ── Cycle time ───────────────────────────────────────────────────────────
    if case_ctx.get("days_in_process") is not None:
        days_in = round(float(case_ctx["days_in_process"]), 2)
        global_avg_str = f"{avg_e2e} days" if avg_e2e else "N/A"
        diff = round(days_in - float(avg_e2e or 0), 1)
        sign = "+" if diff > 0 else ""
        cycle_time = f"{days_in} days ({sign}{diff}d vs global avg {global_avg_str})"
    elif vendor_ctx.get("avg_duration_days") is not None:
        vd = round(float(vendor_ctx["avg_duration_days"]), 2)
        global_avg_str = f"{avg_e2e} days" if avg_e2e else "N/A"
        diff = round(vd - float(avg_e2e or 0), 1)
        sign = "+" if diff > 0 else ""
        cycle_time = f"Vendor avg: {vd} days ({sign}{diff}d vs global {global_avg_str})"
    elif avg_e2e is not None:
        cycle_time = f"Global avg: {avg_e2e} days"
    else:
        cycle_time = "See GLOBAL PROCESS STATS"

    # ── Vendor behavior ──────────────────────────────────────────────────────
    if vendor_ctx:
        exc_rate = vendor_ctx.get("exception_rate_pct", "?")
        total_cases_v = vendor_ctx.get("total_cases", "?")
        avg_dur_v = vendor_ctx.get("avg_duration_days", "?")
        cycle_diff = vendor_ctx.get("duration_vs_overall_days")
        sign = "+" if (cycle_diff or 0) > 0 else ""
        diff_str = f" ({sign}{cycle_diff}d vs global)" if cycle_diff is not None else ""
        vendor_behavior = (
            f"Vendor {vendor_ctx.get('vendor_id', '')}: "
            f"{exc_rate}% exception rate, "
            f"{total_cases_v} cases, "
            f"avg {avg_dur_v} days{diff_str}"
        )
    elif vendor_summary:
        # Global scope: summarize top 2-3 vendors
        top = vendor_summary[:3]
        parts = [
            f"{r.get('vendor_id', '?')}: {r.get('exc_rate', 0):.1f}% exc rate"
            for r in top
        ]
        vendor_behavior = "Top vendors by exception rate: " + "; ".join(parts)
    elif exc_cases:
        # Exception scope: derive from exception case vendor data if available
        active_count = sum(1 for e in exc_cases if e.get("is_active"))
        vendor_behavior = (
            f"Cross-vendor: {active_count} active exceptions across all vendors. "
            f"See VENDOR BEHAVIOUR SUMMARY for per-vendor breakdown."
        )
    elif exc_patterns:
        vendor_behavior = (
            f"Cross-vendor exception analysis: "
            f"{exc_patterns[0].get('exception_type', 'N/A')} most common "
            f"({exc_patterns[0].get('frequency_percentage', '?')}% of cases)"
        )
    else:
        vendor_behavior = f"Global avg cycle time: {avg_e2e} days. No vendor-specific scope active."

    return {
        "process_step": process_step,
        "deviation_point": deviation_point,
        "variant_path": variant_path,
        "cycle_time": cycle_time,
        "vendor_behavior": vendor_behavior,
    }


# ---------------------------------------------------------------------------
# Agent router
# ---------------------------------------------------------------------------

def _pick_agent(message: str, vendor_id: Optional[str], case_id: Optional[str]) -> str:
    q = message.lower()
    if vendor_id:
        return "Vendor Intelligence Agent"
    if case_id:
        return "Invoice Processing Agent"
    if any(x in q for x in ["exception", "error", "block", "stuck", "issue", "overdue", "due date"]):
        return "Exception Detection Agent"
    if any(x in q for x in ["vendor", "supplier", "lifnr"]):
        return "Vendor Intelligence Agent"
    if any(x in q for x in ["conform", "violation", "rule", "breach", "compliance"]):
        return "Conformance Checker Agent"
    if any(x in q for x in ["bottleneck", "delay", "slow", "cycle", "time", "duration", "wait"]):
        return "Process Insight Agent"
    if any(x in q for x in ["recommend", "next", "action", "what should", "suggest", "how to fix"]):
        return "Case Resolution Agent"
    return "Process Intelligence Agent"


# ---------------------------------------------------------------------------
# System prompt  (PI Context Used is now PRE-FILLED with real values)
# ---------------------------------------------------------------------------

_BASE_SYSTEM_PROMPT = """\
You are a Process Intelligence Analyst embedded in an AP invoice operations platform.
You have direct access to the live Celonis event log data shown below.
Speak to operations managers. Every answer is a process investigation -- never a generic chatbot reply.

======================================================
RESPONSE FORMAT  (follow this EXACT structure -- every section is required)
======================================================

**Summary:** [1-2 sentences: what happened, with the single most critical number.]

**PI Context Used:**
- Process step: {pi_ctx_process_step}
- Deviation point: {pi_ctx_deviation_point}
- Variant path: {pi_ctx_variant_path}
- Cycle time: {pi_ctx_cycle_time}
- Vendor behavior: {pi_ctx_vendor_behavior}

**Metrics:**
- Top exceptions: [list each type with % and case count, e.g. "Due Date Passed: 35% (12 cases)"]
- Delays: [specific bottleneck transition and days from TRANSITION DURATIONS -- e.g. "Approval -> Posting: 8.3 days avg"]
- Cases affected: [exact count and % of total]

**Risk & Impact:**
- Payment delay risk: [specific -- e.g. "Net 30 breach on 8 cases if not resolved by Friday"]
- SLA breach: [Yes/No and exactly why -- cite the threshold]
- Manual effort: [estimated rework impact -- e.g. "~4h per AP clerk based on 3-step resolution path"]

**Next Best Action:**
- [Specific action -- not generic. E.g. "Release block on Case INV-4421 in source system"]
- [Who should do it -- e.g. "AP Supervisor (role: APSUPR)"]
- [By when -- e.g. "Within 24h to avoid SLA breach on 6 cases"]

**Similar Cases:**
- [List case IDs that had the same issue, how they resolved, and how long it took. If none found: "No similar resolved cases in current dataset."]

======================================================
CRITICAL RULES
======================================================
1.  Use exact numbers from the data. Write "23.94 days" not "about 24 days".
2.  Bold the 2-3 most critical metrics inline within their sections.
3.  Every claim must cite a specific number from the PI data.
4.  The PI Context Used section above is PRE-FILLED with real values derived
    from the data. REPRODUCE those values EXACTLY as written -- do NOT replace
    them with "[Data unavailable]" or any other placeholder.
5.  Compare to global averages whenever possible.
6.  Reference specific activity names and process paths from the event log.
7.  For "active" exceptions: use the INDIVIDUAL EXCEPTION CASES section.
8.  Answer ONLY from the data below. Never invent numbers.
9.  VENDOR SCOPE: entire answer about that vendor only.
10. CASE SCOPE: entire answer about that case only.
11. NO SCOPE: answer from global process data.
12. Do not rename or reorder the sections above.
13. For Delays in Metrics: ALWAYS use the top entries from TRANSITION DURATIONS section.
    If transition data exists, you MUST cite it -- never write "unavailable".
14. For Vendor behavior: if no vendor scope, summarise the top 2-3 rows from
    VENDOR BEHAVIOUR SUMMARY. Never write "N/A" when that section is present.

======================================================
LIVE CELONIS EVENT LOG DATA
======================================================
{pi_context}

======================================================
ACTIVE SCOPE: {scope_label}
{scope_instruction}
======================================================
"""


# ---------------------------------------------------------------------------
# ChatService
# ---------------------------------------------------------------------------

class ChatService:

    def __init__(
        self,
        llm: AzureOpenAIService,
        celonis: CelonisService,
        process_insight: ProcessInsightService,
        suggestion_service: SuggestionService,
        agent_recommendation=None,
    ):
        self.llm = llm
        self.celonis = celonis
        self.process_insight = process_insight
        self.suggestion_service = suggestion_service

    # -----------------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------------

    def chat(
        self,
        message: str,
        conversation_history: List[Dict[str, str]],
        case_id: Optional[str] = None,
        vendor_id: Optional[str] = None,
    ) -> Dict[str, Any]:

        # ── UNIVERSAL ENTITY DETECTION ───────────────────────────────────
        detected_ids = _extract_all_entity_ids(message)
        logger.info("Detected entity IDs from message: %s", detected_ids)

        # Resolve case_id / vendor_id from entity graph first (most reliable)
        q_lower = message.lower()
        if not case_id and not vendor_id and detected_ids:
            try:
                from app.services.data_cache_service import get_data_cache_service
                cache = get_data_cache_service()
                for eid in detected_ids:
                    node = cache.entity_graph.get(eid.upper())
                    if node:
                        etype = node.get("entity_type", "entity")
                        if etype == "vendor" or "vendor" in q_lower or "supplier" in q_lower:
                            vendor_id = eid
                            logger.info("Entity graph matched vendor_id=%s (type=%s)", eid, etype)
                        else:
                            case_id = eid
                            logger.info("Entity graph matched case_id=%s (type=%s)", eid, etype)
                        break
            except Exception as eg_err:
                logger.warning("Entity graph lookup skipped: %s", eg_err)

        # Legacy fallback: use old regex extractors if entity graph had no match
        if not case_id and not vendor_id:
            case_id = _extract_case_id(message)
            if case_id:
                logger.info("Legacy extractor detected case_id=%s", case_id)
        if not vendor_id:
            vendor_id = _extract_vendor_id(message)
            if vendor_id:
                logger.info("Legacy extractor detected vendor_id=%s", vendor_id)

        # Final fallback: plain number → case_id unless "vendor" in message
        if not case_id and not vendor_id and detected_ids:
            raw_id = detected_ids[0]
            if "vendor" in q_lower or "supplier" in q_lower:
                vendor_id = raw_id
                logger.info("Fallback detected vendor_id=%s", vendor_id)
            else:
                case_id = raw_id
                logger.info("Fallback detected case_id=%s", case_id)

        event_log = self._fetch_event_log_once()

        pi_context, context_used, data_sources, process_ctx = self._build_context(
            case_id=case_id,
            vendor_id=vendor_id,
            event_log=event_log,
            detected_ids=detected_ids,
        )

        vendor_found = context_used.get("vendor") is not None
        case_found = context_used.get("case") is not None
        entity_node = context_used.get("entity_graph_hit")

        if entity_node:
            scope_label = f"Entity {entity_node['entity_id']} ({entity_node['entity_type']})"
            scope_instruction = _ENTITY_SCOPE_INSTRUCTION.format(
                entity_id=entity_node["entity_id"],
                entity_type=entity_node["entity_type"],
            )
        elif vendor_id and case_id:
            scope_label = f"Vendor {vendor_id} + Case {case_id}"
            scope_instruction = (
                _VENDOR_SCOPE_INSTRUCTION.format(vendor_id=vendor_id)
                + _CASE_SCOPE_INSTRUCTION.format(case_id=case_id)
            )
        elif vendor_id:
            found_str = "" if vendor_found else " [NOT FOUND IN DATA]"
            scope_label = f"Vendor {vendor_id}{found_str}"
            scope_instruction = _VENDOR_SCOPE_INSTRUCTION.format(vendor_id=vendor_id)
        elif case_id:
            found_str = "" if case_found else " [NOT FOUND IN DATA]"
            scope_label = f"Case {case_id}{found_str}"
            scope_instruction = _CASE_SCOPE_INSTRUCTION.format(case_id=case_id)
        else:
            scope_label = "Global -- all cases and vendors"
            scope_instruction = ""

        agent_used = _pick_agent(message, vendor_id, case_id)

        # ── DERIVE PI CONTEXT FIELDS (guaranteed non-empty for any query) ──
        pi_ctx_fields = _derive_pi_context_fields(
            message=message,
            context_used=context_used,
            process_ctx=process_ctx,
            event_log=event_log,
        )

        wants_graph = _user_wants_graph(message)
        graph_path: Optional[str] = None
        path_label: Optional[str] = None
        if wants_graph:
            graph_path, path_label = self._pick_graph_path(
                message=message,
                process_ctx=process_ctx,
                vendor_id=vendor_id,
                context_used=context_used,
            )

        system_prompt = _BASE_SYSTEM_PROMPT.format(
            pi_context=pi_context or "[No event log data available -- check Celonis connection]",
            scope_label=scope_label,
            scope_instruction=scope_instruction,
            # Pre-filled PI Context Used values
            pi_ctx_process_step=pi_ctx_fields["process_step"],
            pi_ctx_deviation_point=pi_ctx_fields["deviation_point"],
            pi_ctx_variant_path=pi_ctx_fields["variant_path"],
            pi_ctx_cycle_time=pi_ctx_fields["cycle_time"],
            pi_ctx_vendor_behavior=pi_ctx_fields["vendor_behavior"],
        )
        user_prompt = self._build_user_prompt(message, conversation_history)

        try:
            reply = self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
                max_tokens=2500,
            )
        except Exception as exc:
            logger.error("LLM call failed: %s", exc)
            return self._error_response(str(exc), scope_label, agent_used, data_sources, context_used)

        suggested_questions: List[str] = []
        try:
            suggested_questions = self.suggestion_service.generate_suggestions(
                user_message=message,
                ai_reply=reply,
                context_used=context_used,
                case_id=case_id,
                vendor_id=vendor_id,
            )
        except Exception as e:
            logger.warning("Suggestion generation failed: %s", e)

        next_steps = self._generate_next_steps(message, context_used, case_id, vendor_id)

        pi_evidence = self._extract_pi_evidence(
            context_used=context_used,
            process_ctx=process_ctx,
            event_log=event_log,
            graph_path=graph_path,
            path_label=path_label,
            pi_ctx_fields=pi_ctx_fields,       # <-- pass derived fields
        )

        return {
            "success": True,
            "reply": reply,
            "suggested_questions": suggested_questions,
            "data_sources": data_sources,
            "next_steps": next_steps,
            "context_used": context_used,
            "scope_label": scope_label,
            "agent_used": agent_used,
            "error": None,
            "pi_evidence": pi_evidence,
            "similar_cases": context_used.get("similar_cases") or None,
            "vendor_context": context_used.get("vendor"),
            "graph_path": graph_path,
            "path_label": path_label,
        }

    # -----------------------------------------------------------------------
    # Event log -- single fetch
    # -----------------------------------------------------------------------

    def _fetch_event_log_once(self) -> pd.DataFrame:
        try:
            from app.services.data_cache_service import get_data_cache_service
            cache = get_data_cache_service()
            if not cache._is_loaded:
                cache.ensure_loaded()
            if cache._is_loaded:
                df = cache.get_event_log()
                if not df.empty:
                    return df.copy()

            df = self.celonis.get_event_log()
            if df is None or df.empty:
                logger.warning("Event log returned empty")
                return pd.DataFrame()
            for col in ("case_id", "activity", "timestamp"):
                if col not in df.columns:
                    logger.error("Event log missing required column: %s", col)
                    return pd.DataFrame()
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            return df
        except Exception as exc:
            logger.error("Failed to fetch event log: %s", exc)
            return pd.DataFrame()

    # -----------------------------------------------------------------------
    # Context builder
    # -----------------------------------------------------------------------

    def _build_context(
        self,
        case_id: Optional[str],
        vendor_id: Optional[str],
        event_log: pd.DataFrame,
        detected_ids: Optional[List[str]] = None,
    ) -> Tuple[str, Dict[str, Any], List[str], Optional[Dict[str, Any]]]:

        context_used: Dict[str, Any] = {}
        sections: List[str] = []
        data_sources: List[str] = []
        process_ctx: Optional[Dict] = None
        detected_ids = detected_ids or []

        # ------------------------------------------------------------------
        # ENTITY GRAPH LOOKUP — inject full entity JSON before all else
        # ------------------------------------------------------------------
        try:
            from app.services.data_cache_service import get_data_cache_service
            _cache = get_data_cache_service()
            # Build candidate ID list: detected_ids + case_id + vendor_id
            candidates = list(dict.fromkeys(
                [i.upper() for i in detected_ids if i]
                + ([case_id.upper()] if case_id else [])
                + ([vendor_id.upper()] if vendor_id else [])
            ))
            for eid in candidates:
                node = _cache.entity_graph.get(eid)
                if node:
                    entity_json_str = json.dumps(node, indent=2, default=str)
                    sections.append(
                        f"ENTITY DATA FROM CELONIS KNOWLEDGE MODEL\n"
                        f"(Searched ID: {eid} | Table: {node.get('source_table')} | Type: {node.get('entity_type')})\n"
                        f"```json\n{entity_json_str}\n```"
                    )
                    context_used["entity_graph_hit"] = node
                    data_sources.append(
                        f"Entity Graph — {node.get('source_table', '?')} "
                        f"(ID: {eid}, type: {node.get('entity_type', '?')})"
                    )
                    logger.info(
                        "Entity graph hit: id=%s table=%s type=%s related=%d",
                        eid, node.get('source_table'), node.get('entity_type'),
                        len(node.get('related', [])),
                    )
                    break   # use first match only; others surface in related[]
        except Exception as eg_ctx_err:
            logger.warning("Entity graph context injection failed: %s", eg_ctx_err)

        transition_durations: Dict[str, float] = {}
        activity_durations: Dict[str, float] = {}
        if not event_log.empty:
            transition_durations = _compute_transition_durations(event_log)
            activity_durations = _compute_activity_durations(event_log)

        # ------------------------------------------------------------------
        # Global process statistics
        # ------------------------------------------------------------------
        try:
            from app.services.data_cache_service import get_data_cache_service
            cache = get_data_cache_service()
            if not cache._is_loaded:
                cache.ensure_loaded()
            if cache._is_loaded:
                process_ctx = cache.get_process_context()
            else:
                process_ctx = self.process_insight.build_process_context()

            if process_ctx is not None:
                if not process_ctx.get("transition_durations"):
                    process_ctx["transition_durations"] = transition_durations
                if not process_ctx.get("activity_durations"):
                    process_ctx["activity_durations"] = activity_durations

            bottleneck = process_ctx.get("bottleneck", {})
            variants = process_ctx.get("variants", [])
            exc_patterns = process_ctx.get("exception_patterns", [])
            conformance = process_ctx.get("conformance_violations", [])
            dec_rules = process_ctx.get("decision_rules", [])
            total_cases = process_ctx.get("total_cases", 0)
            total_events = process_ctx.get("total_events", 0)
            avg_e2e = process_ctx.get("avg_end_to_end_days", "N/A")

            global_section = (
                f"GLOBAL PROCESS STATS  [Celonis Event Log -- {total_events} events, {total_cases} cases]\n"
                f"  Avg end-to-end (days)    : {avg_e2e}\n"
                f"  Exception rate           : {process_ctx.get('exception_rate', 'N/A')}%\n"
                f"  Main bottleneck          : {bottleneck.get('activity', 'N/A')} "
                f"({bottleneck.get('duration_days', 'N/A')} days avg, "
                f"{bottleneck.get('case_count', 'N/A')} cases)\n"
                f"  Golden path              : {process_ctx.get('golden_path', 'N/A')} "
                f"({process_ctx.get('golden_path_percentage', 0)}% of cases)\n"
            )

            if variants:
                global_section += "  Process variants (top 5):\n"
                for v in variants[:5]:
                    global_section += (
                        f"    - {v['variant']} "
                        f"({v['frequency']} cases, {v['percentage']}%)\n"
                    )

            if exc_patterns:
                global_section += f"  Exception patterns ({len(exc_patterns)} types):\n"
                for p in exc_patterns:
                    global_section += (
                        f"    - {p['exception_type']}: {p['frequency_percentage']}% of cases, "
                        f"{p['case_count']} cases, avg resolution {p['avg_resolution_time_days']} days, "
                        f"trigger: {p.get('trigger_condition', '?')}, "
                        f"resolved by: {p.get('typical_resolution', '?')} "
                        f"(role: {p.get('resolution_role', '?')})\n"
                    )
            else:
                global_section += "  Exception patterns: None detected\n"

            if conformance:
                global_section += f"  Conformance violations ({len(conformance)} found):\n"
                for v in conformance:
                    global_section += (
                        f"    - {v['rule']}: {v['violation_rate']}% "
                        f"({v['affected_cases']} / {v['total_cases']} cases) -- {v['violation_description']}\n"
                    )
            else:
                global_section += "  Conformance violations: None\n"

            if dec_rules:
                global_section += "  Decision rules:\n"
                for r in dec_rules:
                    global_section += (
                        f"    - {r['condition']} -> {r['action']} "
                        f"(confidence: {r['confidence']})\n"
                    )

            if "all_tables_extract" in process_ctx:
                extract = process_ctx["all_tables_extract"]
                if extract.get("tables"):
                    global_section += f"\n  ALL TABLES SUMMARY (Celonis Knowledge Model - {extract.get('total_tables', 0)} tables):\n"
                    for t in extract["tables"]:
                        row_c = t.get("row_count", 0)
                        amt = t.get("amount_summary", {})
                        total_amt = amt.get("total_amount_calculated", 0)
                        amt_str = f", Amount Total: {total_amt}" if total_amt else ""
                        global_section += f"    - {t.get('name')}: {row_c} rows{amt_str}\n"

            sections.append(global_section)
            context_used["global"] = {
                "total_cases": total_cases,
                "total_events": total_events,
                "avg_end_to_end_days": avg_e2e,
                "exception_rate": process_ctx.get("exception_rate"),
                "bottleneck": bottleneck,
                "conformance_count": len(conformance),
                "exception_type_count": len(exc_patterns),
                "exception_patterns": exc_patterns,
                "variants": variants[:5],
                "transition_durations": transition_durations,
                "activity_durations": activity_durations,
                "all_tables_extract": process_ctx.get("all_tables_extract"),
            }

            data_sources.append(f"Celonis Event Log -- {total_cases} cases, {total_events} events")
            if "all_tables_extract" in process_ctx and process_ctx["all_tables_extract"].get("tables"):
                data_sources.append(f"Celonis Knowledge Model -- {len(process_ctx['all_tables_extract']['tables'])} tables available")
            if variants:
                data_sources.append(f"Process Variants -- {len(variants)} paths mined")
            if exc_patterns:
                data_sources.append(f"Exception Patterns -- {len(exc_patterns)} types")
            if conformance:
                data_sources.append(f"Conformance -- {len(conformance)} violations")

        except Exception as exc:
            logger.warning("Global process context failed: %s", exc)
            sections.append("GLOBAL PROCESS STATS\n  [Unavailable -- check ProcessInsightService]\n")

        # Transition durations section
        if transition_durations:
            trans_section = f"TRANSITION DURATIONS  (avg days between consecutive activities, all cases)\n"
            for trans, days in list(transition_durations.items())[:20]:
                bar = "█" * min(int(days), 20)
                trans_section += f"  {days:>7.2f}d  {bar}  {trans}\n"
            sections.append(trans_section)
            data_sources.append(f"Transition Durations -- {len(transition_durations)} pairs computed")

        # Activity durations section
        if activity_durations:
            act_section = f"ACTIVITY DURATIONS  (avg days spent at each process step)\n"
            for act, days in list(activity_durations.items())[:15]:
                act_section += f"  {days:>7.2f}d  {act}\n"
            sections.append(act_section)

        # Vendor behaviour summary (always, not just when no vendor scope)
        if not event_log.empty:
            vendor_summary = _compute_vendor_summary(event_log, top_n=10)
            if vendor_summary:
                vs_section = (
                    f"VENDOR BEHAVIOUR SUMMARY  (top vendors by exception rate)\n"
                    f"  {'Vendor ID':<15} {'Total Cases':>12} {'Exc Cases':>10} {'Exc Rate':>10}\n"
                    f"  {'-'*50}\n"
                )
                for row in vendor_summary:
                    vs_section += (
                        f"  {str(row.get('vendor_id', '?')):<15} "
                        f"{int(row.get('total_cases', 0)):>12} "
                        f"{int(row.get('exc_cases', 0)):>10} "
                        f"{row.get('exc_rate', 0):>9.2f}%\n"
                    )
                sections.append(vs_section)
                context_used["vendor_summary"] = vendor_summary
                data_sources.append(f"Vendor Summary -- {len(vendor_summary)} vendors ranked")

        # Individual exception cases
        if not event_log.empty:
            try:
                exc_section, exc_list = self._build_exception_case_details(event_log)
                if exc_section:
                    sections.append(exc_section)
                    context_used["exception_cases"] = exc_list
                    data_sources.append(f"Exception Case Details -- {len(exc_list)} individual cases")
            except Exception as exc:
                logger.warning("Exception case details failed: %s", exc)

        # Known vendor IDs
        known_vendor_ids = self._get_known_vendor_ids(event_log)
        if known_vendor_ids:
            vid_section = (
                f"KNOWN VENDOR IDs IN DATA ({len(known_vendor_ids)} vendors):\n"
                + "".join(f"  - {v}\n" for v in known_vendor_ids[:40])
            )
            if len(known_vendor_ids) > 40:
                vid_section += f"  ... and {len(known_vendor_ids) - 40} more\n"
            sections.append(vid_section)
            context_used["known_vendor_ids"] = known_vendor_ids

        # Case-specific context
        if case_id and not event_log.empty:
            case_section, case_ctx, similar = self._build_case_context(
                case_id=case_id,
                event_log=event_log,
                process_ctx=process_ctx,
            )
            sections.append(case_section)
            if case_ctx:
                context_used["case"] = case_ctx
                context_used["similar_cases"] = similar
                data_sources.append(
                    f"Case {case_id} -- {case_ctx['activity_count']} activities, "
                    f"current: {case_ctx['current_stage']}"
                )
                if similar:
                    data_sources.append(f"Similar Cases -- {len(similar)} cases with matching path")

        # Vendor-specific context
        if vendor_id:
            vendor_section, vendor_snapshot = self._build_vendor_context(
                vendor_id=vendor_id,
                event_log=event_log,
                process_ctx=process_ctx,
                known_vendor_ids=known_vendor_ids,
            )
            sections.append(vendor_section)
            if vendor_snapshot:
                context_used["vendor"] = vendor_snapshot
                data_sources.append(
                    f"Vendor {vendor_id} -- {vendor_snapshot.get('total_cases', 0)} cases, "
                    f"{vendor_snapshot.get('exception_rate_pct', 0)}% exception rate"
                )

        return "\n\n".join(sections), context_used, data_sources, process_ctx

    # -----------------------------------------------------------------------
    # Case context builder
    # -----------------------------------------------------------------------

    def _build_case_context(
        self,
        case_id: str,
        event_log: pd.DataFrame,
        process_ctx: Optional[Dict],
    ) -> Tuple[str, Optional[Dict], List[Dict]]:
        lookup = _normalise_id(case_id)
        norm_col = _safe_str_col(event_log["case_id"]).str.strip().str.upper()
        case_events = pd.DataFrame()
        match_strategy = ""

        for strategy, mask_fn in [
            ("exact",        lambda: norm_col == lookup),
            ("contains",     lambda: norm_col.str.contains(lookup, na=False, regex=False)),
            ("strip_V",      lambda: norm_col == lookup.lstrip("V")),
            ("suffix8",      lambda: norm_col.str.endswith(lookup[-8:], na=False)),
            ("numeric",      lambda: norm_col.apply(lambda v: re.sub(r'\D', '', v))
                                     == re.sub(r'\D', '', lookup)),
        ]:
            mask = mask_fn()
            if mask.any():
                case_events = event_log[mask]
                match_strategy = strategy
                break

        if case_events.empty:
            core_m = re.search(r'V\d+', lookup)
            if core_m:
                core = core_m.group(0)
                norm_core = norm_col.apply(
                    lambda v: (re.search(r'V\d+', v) or type('', (), {'group': lambda *_: v})()).group(0)
                )
                mask = norm_core == core
                if mask.any():
                    case_events = event_log[mask]
                    match_strategy = f"core_V({core})"

        sample_ids = norm_col.unique()[:10].tolist()

        if case_events.empty:
            extra_info = ""
            try:
                from app.services.data_cache_service import get_data_cache_service
                cache = get_data_cache_service()
                
                for name, df in [
                    ("Purchase Headers", cache.purchasing_header_df),
                    ("Full Case Table", cache.case_table_full_df),
                    ("WCM OLAP", cache.wcm_olap_df)
                ]:
                    if df is not None and not df.empty:
                        # Search all columns for the ID
                        mask = pd.Series([False] * len(df), index=df.index)
                        for c in df.columns[:15]: # Limit to first 15 cols to be fast
                            ser = _safe_str_col(df[c]).str.strip().str.upper()
                            mask = mask | (ser == lookup) | ser.str.contains(lookup, na=False, regex=False)
                        
                        match_df = df[mask]
                        if not match_df.empty:
                            sample = match_df.head(1).to_dict('records')[0]
                            compact = {str(k): (str(v)[:100] + "..." if len(str(v)) > 100 else str(v)) for k, v in sample.items() if pd.notna(v) and str(v).strip() and str(v) != "nan" and str(v) != "None"}
                            extra_info += f"  - Found {len(match_df)} row(s) in '{name}'.\n    Data: {compact}\n"
            except Exception as e:
                logger.warning(f"Failed to search extra tables: {e}")

            if extra_info:
                section = (
                    f"CASE DETAIL: {case_id}\n"
                    f"  NOT FOUND in the Celonis event log trace.\n"
                    f"  HOWEVER, FOUND IN OTHER CELONIS DATA TABLES:\n{extra_info}"
                )
                case_ctx = {
                    "case_id": case_id,
                    "current_stage": "Unknown",
                    "days_in_process": None,
                    "activity_count": 0,
                    "variant": "N/A",
                    "match_strategy": "aux_table_search",
                    "is_exception": False,
                    "global_avg_days": 0,
                }
                return section, case_ctx, []
            else:
                section = (
                    f"CASE DETAIL: {case_id}\n"
                    f"  NOT FOUND in the Celonis event log or any auxiliary data tables.\n"
                    f"  Total distinct cases in log : {event_log['case_id'].nunique() if 'case_id' in event_log.columns else 0}\n"
                    f"  Sample case IDs             : {sample_ids}\n"
                    f"  Strategies attempted        : exact, contains, core_V, strip_V, suffix, numeric\n"
                )
                return section, None, []

        sorted_ev = case_events.sort_values("timestamp")
        activities = sorted_ev["activity"].tolist()
        current = activities[-1] if activities else "Unknown"
        start_ts = sorted_ev["timestamp"].min()
        end_ts = sorted_ev["timestamp"].max()
        days_in = (
            (end_ts - start_ts).total_seconds() / 86400
            if pd.notnull(start_ts) and pd.notnull(end_ts) else None
        )
        case_variant = " -> ".join(activities)
        global_avg = float(process_ctx.get("avg_end_to_end_days", 0) or 0) if process_ctx else 0
        comparison = ""
        if days_in is not None and global_avg:
            diff = round(days_in - global_avg, 1)
            comparison = f"  ({'+' if diff > 0 else ''}{diff}d vs global avg {global_avg}d)"

        is_exception = bool(_EXCEPTION_PATTERN.search(current))
        exc_flag = " *** CURRENTLY IN EXCEPTION ***" if is_exception else ""

        similar = self._find_similar_cases(event_log, case_id, case_variant)

        section = (
            f"CASE DETAIL: {case_id}  [match: {match_strategy}]\n"
            f"  Current stage      : {current}{exc_flag}\n"
            f"  Days in process    : {round(days_in, 2) if days_in is not None else 'N/A'}{comparison}\n"
            f"  Global avg (days)  : {global_avg}\n"
            f"  Total activities   : {len(activities)}\n"
            f"  Full process path  : {case_variant}\n"
            f"  Similar cases      : {len(similar)} cases follow the same path\n"
        )

        case_ctx = {
            "case_id": case_id,
            "current_stage": current,
            "days_in_process": days_in,
            "activity_count": len(activities),
            "variant": case_variant,
            "match_strategy": match_strategy,
            "is_exception": is_exception,
            "global_avg_days": global_avg,
        }
        return section, case_ctx, similar

    # -----------------------------------------------------------------------
    # Vendor context builder
    # -----------------------------------------------------------------------

    def _build_vendor_context(
        self,
        vendor_id: str,
        event_log: pd.DataFrame,
        process_ctx: Optional[Dict],
        known_vendor_ids: List[str],
    ) -> Tuple[str, Optional[Dict]]:
        snapshot = self._build_vendor_snapshot(vendor_id, event_log, process_ctx)

        if snapshot:
            global_exc = snapshot.get("overall_exception_rate_pct", 0) or 0
            global_days = snapshot.get("overall_avg_duration_days", 0) or 0
            dur_diff = round(float(snapshot.get("avg_duration_days", 0) or 0) - float(global_days), 2)
            exc_diff = round(float(snapshot.get("exception_rate_pct", 0) or 0) - float(global_exc), 2)
            dur_sign = "+" if dur_diff > 0 else ""
            exc_sign = "+" if exc_diff > 0 else ""

            section = (
                f"VENDOR SNAPSHOT: {vendor_id}  [Celonis Event Log]\n"
                f"  Total cases          : {snapshot.get('total_cases', 'N/A')}\n"
                f"  Exception rate       : {snapshot.get('exception_rate_pct', 'N/A')}%  "
                f"(global: {global_exc}%,  delta: {exc_sign}{exc_diff}%)\n"
                f"  Avg cycle time       : {snapshot.get('avg_duration_days', 'N/A')} days  "
                f"(global: {global_days} days,  delta: {dur_sign}{dur_diff}d)\n"
                f"  Exception cases      : {snapshot.get('exception_case_count', 'N/A')}\n"
                f"  Most common variant  : {snapshot.get('most_common_variant', 'N/A')}\n"
                f"  Payment terms        : {snapshot.get('payment_terms', 'N/A')}\n"
                f"  Currency             : {snapshot.get('currency', 'N/A')}\n"
            )
            exc_types = snapshot.get("top_exception_types", [])
            if exc_types:
                section += "  Top exception types (this vendor):\n"
                for e in exc_types:
                    section += (
                        f"    - {e['exception_type']}: {e['case_count']} cases "
                        f"({e.get('pct', '?')}% of vendor cases)\n"
                    )
        else:
            sample_vids = known_vendor_ids[:15]
            section = (
                f"VENDOR SNAPSHOT: {vendor_id}\n"
                f"  NOT FOUND in the Celonis event log.\n"
                f"  Possible reasons:\n"
                f"    1. vendor_id column not joined into event log extract.\n"
                f"    2. ID format mismatch -- "
                f"try: {vendor_id.zfill(10)} or {vendor_id.lstrip('0')}\n"
                f"    3. This vendor has no invoices in the current dataset.\n"
                f"  Known vendor IDs (sample): {sample_vids}\n"
            )

        return section, snapshot

    def _build_vendor_snapshot(
        self,
        vendor_id: str,
        event_log: pd.DataFrame,
        process_ctx: Optional[Dict],
    ) -> Optional[Dict]:
        lookup = vendor_id.strip().upper()

        vendor_stats = process_ctx.get("vendor_stats", []) if process_ctx else []
        row = next(
            (v for v in vendor_stats if _normalise_id(v.get("vendor_id", "")) == lookup),
            None,
        )
        if row:
            logger.info("Vendor %s found in process_ctx.vendor_stats", vendor_id)
            return self._enrich_vendor_row(dict(row), event_log, process_ctx)

        vendor_df = self._find_vendor_in_logs(lookup, event_log)
        if vendor_df is None or vendor_df.empty:
            logger.warning("Vendor %s not found. Known (sample): %s",
                           vendor_id, self._get_known_vendor_ids(event_log)[:5])
            return None

        logger.info("Vendor %s -- %d rows", vendor_id, len(vendor_df))
        total_cases = int(vendor_df["case_id"].nunique())

        exc_mask = _safe_str_col(vendor_df["activity"]).str.contains(
            _EXCEPTION_PATTERN.pattern, case=False, na=False, regex=True
        )
        exc_cases = int(vendor_df[exc_mask]["case_id"].nunique())

        case_dur = (
            vendor_df.groupby("case_id", sort=False)
            .agg(start=("timestamp", "min"), end=("timestamp", "max"))
            .reset_index()
        )
        case_dur["dur"] = (case_dur["end"] - case_dur["start"]).dt.total_seconds() / 86400
        avg_dur = round(float(case_dur["dur"].mean()), 2) if len(case_dur) > 0 else 0.0

        most_common_variant = ""
        try:
            variants_series = (
                vendor_df.sort_values(["case_id", "timestamp"])
                .groupby("case_id", sort=False)["activity"]
                .apply(lambda x: " -> ".join(x.astype(str).tolist()))
                .value_counts()
            )
            if not variants_series.empty:
                most_common_variant = str(variants_series.index[0])
        except Exception:
            pass

        payment_terms = self._extract_column_value(
            vendor_df, ["payment_terms", "PAYMENT_TERMS", "zterm", "ZTERM", "PaymentTerms"]
        )
        currency = self._extract_column_value(
            vendor_df, ["currency", "CURRENCY", "waers", "WAERS", "Currency"]
        )

        vendor_exc_activities = vendor_df[exc_mask]["activity"].value_counts().head(5)
        top_exception_types = [
            {
                "exception_type": act,
                "case_count": int(cnt),
                "pct": round(cnt / total_cases * 100, 1) if total_cases else 0,
            }
            for act, cnt in vendor_exc_activities.items()
        ]

        row = {
            "vendor_id": vendor_id,
            "total_cases": total_cases,
            "exception_case_count": exc_cases,
            "exception_rate_pct": round(exc_cases / total_cases * 100, 2) if total_cases else 0.0,
            "avg_duration_days": avg_dur,
            "most_common_variant": most_common_variant,
            "payment_terms": payment_terms,
            "currency": currency,
            "top_exception_types": top_exception_types,
        }
        return self._enrich_vendor_row(row, event_log, process_ctx)

    def _find_vendor_in_logs(self, lookup: str, event_log: pd.DataFrame) -> Optional[pd.DataFrame]:
        enriched: Optional[pd.DataFrame] = None
        try:
            enriched = self.celonis.get_event_log_with_vendor()
            if enriched is None or enriched.empty:
                enriched = None
        except Exception as exc:
            logger.warning("get_event_log_with_vendor failed: %s", exc)

        candidates = []
        if enriched is not None and "vendor_id" in enriched.columns:
            candidates.append(enriched)
        if "vendor_id" in event_log.columns:
            candidates.append(event_log)

        for df in candidates:
            norm = _safe_str_col(df["vendor_id"]).str.strip().str.upper()
            for candidate in [lookup, lookup.lstrip("0"), lookup.zfill(10)]:
                if candidate and candidate != lookup or candidate == lookup:
                    mask = norm == candidate
                    if mask.any():
                        return df[mask].copy()
        return None

    @staticmethod
    def _enrich_vendor_row(row: Dict, event_log: pd.DataFrame, process_ctx: Optional[Dict]) -> Dict:
        if process_ctx:
            overall_avg = float(process_ctx.get("avg_end_to_end_days", 0) or 0)
            overall_exc = float(process_ctx.get("exception_rate", 0) or 0)
        else:
            overall_avg, overall_exc = 0.0, 0.0
        row["overall_avg_duration_days"] = overall_avg
        row["overall_exception_rate_pct"] = overall_exc
        row["duration_vs_overall_days"] = round(
            float(row.get("avg_duration_days", 0) or 0) - overall_avg, 2
        )
        return row

    @staticmethod
    def _extract_column_value(df: pd.DataFrame, col_names: List[str]) -> str:
        for col in col_names:
            if col in df.columns:
                vals = df[col].dropna()
                if not vals.empty:
                    return str(vals.iloc[0])
        return ""

    def _get_known_vendor_ids(self, event_log: pd.DataFrame) -> List[str]:
        for df in [event_log]:
            if "vendor_id" in df.columns:
                ids = (
                    _safe_str_col(df["vendor_id"]).str.strip()
                    .replace("", pd.NA).dropna().unique().tolist()
                )
                ids = [v for v in ids if v.lower() not in ("nan", "none", "")]
                if ids:
                    return sorted(ids)
        try:
            enriched = self.celonis.get_event_log_with_vendor()
            if enriched is not None and not enriched.empty and "vendor_id" in enriched.columns:
                ids = (
                    _safe_str_col(enriched["vendor_id"]).str.strip()
                    .replace("", pd.NA).dropna().unique().tolist()
                )
                return sorted(v for v in ids if v.lower() not in ("nan", "none", ""))
        except Exception:
            pass
        return []

    # -----------------------------------------------------------------------
    # Exception case details
    # -----------------------------------------------------------------------

    def _build_exception_case_details(
        self,
        event_log: pd.DataFrame,
        max_cases: int = 25,
    ) -> Tuple[str, List[Dict]]:
        if event_log.empty:
            return "", []

        act_col = _safe_str_col(event_log["activity"])
        exc_mask = act_col.str.contains(_EXCEPTION_PATTERN.pattern, case=False, na=False, regex=True)
        exc_events = event_log[exc_mask]
        if exc_events.empty:
            return "", []

        now = pd.Timestamp.now(tz=None)
        exc_case_ids = exc_events["case_id"].unique()
        details: List[Dict] = []

        for cid in exc_case_ids:
            case_ev = event_log[event_log["case_id"] == cid].sort_values("timestamp")
            case_exc = exc_events[exc_events["case_id"] == cid].sort_values("timestamp")
            if case_ev.empty:
                continue

            exc_row = case_exc.iloc[-1]
            last_row = case_ev.iloc[-1]
            is_active = bool(_EXCEPTION_PATTERN.search(str(last_row["activity"])))

            exc_ts = exc_row["timestamp"]
            days_since = None
            if pd.notnull(exc_ts):
                try:
                    ts = exc_ts.tz_localize(None) if exc_ts.tzinfo else exc_ts
                    days_since = round((now - ts).total_seconds() / 86400, 1)
                except Exception:
                    pass

            details.append({
                "case_id": str(cid),
                "exception_type": str(exc_row["activity"]),
                "exception_time": exc_ts,
                "days_since_exception": days_since,
                "current_stage": str(last_row["activity"]),
                "last_activity_time": last_row["timestamp"],
                "is_active": is_active,
                "event_count": len(case_ev),
            })

        details.sort(
            key=lambda x: x["exception_time"]
            if pd.notnull(x["exception_time"]) else pd.Timestamp.min,
            reverse=True,
        )

        active_count = sum(1 for d in details if d["is_active"])
        resolved_count = len(details) - active_count
        show = details[:max_cases]

        section = (
            f"INDIVIDUAL EXCEPTION CASES  "
            f"({len(details)} total: {active_count} ACTIVE, {resolved_count} resolved)\n"
        )
        for d in show:
            status = "ACTIVE" if d["is_active"] else "Resolved"
            exc_ts_str = d["exception_time"].strftime("%Y-%m-%d %H:%M") if pd.notnull(d["exception_time"]) else "N/A"
            last_ts_str = d["last_activity_time"].strftime("%Y-%m-%d %H:%M") if pd.notnull(d["last_activity_time"]) else "N/A"
            days_str = f" ({d['days_since_exception']}d ago)" if d["days_since_exception"] is not None else ""
            section += (
                f"  - {d['case_id']}: {d['exception_type']} "
                f"(at {exc_ts_str}{days_str}) -> now: {d['current_stage']} ({last_ts_str}) [{status}]\n"
            )

        ctx_list = [
            {
                "case_id": d["case_id"],
                "exception_type": d["exception_type"],
                "exception_time": str(d["exception_time"]) if pd.notnull(d["exception_time"]) else None,
                "days_since_exception": d["days_since_exception"],
                "current_stage": d["current_stage"],
                "last_activity_time": str(d["last_activity_time"]) if pd.notnull(d["last_activity_time"]) else None,
                "is_active": d["is_active"],
            }
            for d in show
        ]
        return section, ctx_list

    # -----------------------------------------------------------------------
    # Similar cases
    # -----------------------------------------------------------------------

    def _find_similar_cases(
        self,
        event_log: pd.DataFrame,
        case_id: str,
        case_variant: str,
        max_results: int = 5,
    ) -> List[Dict]:
        try:
            variant_map = (
                event_log.sort_values(["case_id", "timestamp"])
                .groupby("case_id", sort=False)["activity"]
                .apply(lambda x: " -> ".join(x.astype(str).tolist()))
            )
            same = variant_map[
                (variant_map == case_variant) &
                (variant_map.index.astype(str).str.strip().str.upper()
                 != _normalise_id(case_id))
            ]
            if same.empty:
                return []

            dur_map = (
                event_log.groupby("case_id", sort=False)
                .agg(s=("timestamp", "min"), e=("timestamp", "max"))
            )
            dur_map["dur"] = (dur_map["e"] - dur_map["s"]).dt.total_seconds() / 86400

            result = []
            for cid in same.index[:max_results]:
                last_act = event_log[event_log["case_id"] == cid].sort_values("timestamp")
                last_activity = str(last_act.iloc[-1]["activity"]) if not last_act.empty else "Unknown"
                dur = round(float(dur_map.loc[cid, "dur"]), 2) if cid in dur_map.index else None
                result.append({
                    "case_id": str(cid),
                    "duration_days": dur,
                    "current_stage": last_activity,
                    "variant_match": True,
                })
            return result
        except Exception as exc:
            logger.warning("Similar cases lookup failed: %s", exc)
            return []

    # -----------------------------------------------------------------------
    # Graph path picker
    # -----------------------------------------------------------------------

    def _pick_graph_path(
        self,
        message: str,
        process_ctx: Optional[Dict],
        vendor_id: Optional[str],
        context_used: Optional[Dict] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        if not process_ctx:
            return None, None

        context_used = context_used or {}
        q = message.lower()
        golden = process_ctx.get("golden_path", "")
        golden_pct = process_ctx.get("golden_path_percentage", 0)
        variants = process_ctx.get("variants", [])
        bottleneck = process_ctx.get("bottleneck", {})
        bn_activity = bottleneck.get("activity", "")

        c_ctx = context_used.get("case", {})
        if c_ctx and c_ctx.get("variant"):
            case_id = c_ctx.get("case_id", "")
            days = c_ctx.get("days_in_process")
            days_str = f" - {round(days, 1)}d" if days else ""
            stage = c_ctx.get("current_stage", "")
            exc_flag = " EXCEPTION" if c_ctx.get("is_exception") else ""
            return c_ctx["variant"], f"Case {case_id}{days_str} - {stage}{exc_flag}"

        if vendor_id:
            v_ctx = context_used.get("vendor", {})
            v_variant = v_ctx.get("most_common_variant", "")
            if v_variant:
                return v_variant, f"Vendor {vendor_id} - most common path"
            if golden:
                return golden, f"Global most common path - {golden_pct}% of cases"
            return None, None

        if any(x in q for x in ["exception", "rework", "block", "error", "stuck", "due date", "overdue"]):
            for v in variants:
                path = v.get("variant", "")
                if _EXCEPTION_PATTERN.search(path):
                    return path, f"Exception path - {v.get('frequency', '?')} cases ({v.get('percentage', '?')}%)"
            if golden:
                return golden, f"Golden path - {golden_pct}% of cases (no exception variant in top 5)"
            return None, None

        if any(x in q for x in ["bottleneck", "delay", "slow", "wait"]):
            if bn_activity:
                for v in variants:
                    if bn_activity.lower() in v.get("variant", "").lower():
                        return v["variant"], f"Bottleneck path through '{bn_activity}' - {v.get('frequency', '?')} cases"
            if golden:
                return golden, f"Most common path - {golden_pct}% of cases (bottleneck: {bn_activity})"
            return None, None

        if any(x in q for x in ["conform", "violation", "breach", "rule"]):
            for v in variants:
                path = v.get("variant", "")
                if "violation" in path.lower() or "bypass" in path.lower():
                    return path, f"Non-conformant path - {v.get('frequency', '?')} cases"
            if golden:
                return golden, f"Standard conformant path - {golden_pct}% of cases"
            return None, None

        if golden:
            return golden, f"Golden path - {golden_pct}% of cases"
        return None, None

    # -----------------------------------------------------------------------
    # PI Evidence extraction  (now receives pre-derived pi_ctx_fields)
    # -----------------------------------------------------------------------

    def _extract_pi_evidence(
        self,
        context_used: Dict,
        process_ctx: Optional[Dict],
        event_log: pd.DataFrame,
        graph_path: Optional[str] = None,
        path_label: Optional[str] = None,
        pi_ctx_fields: Optional[Dict[str, str]] = None,
    ) -> Dict:
        evidence: Dict = {}
        glob = context_used.get("global", {})
        if not glob:
            return evidence

        total_cases = glob.get("total_cases", 0)
        total_events = glob.get("total_events", 0)
        avg_e2e = glob.get("avg_end_to_end_days")
        exc_rate = glob.get("exception_rate")
        bottleneck = glob.get("bottleneck", {})

        # ── PI Context Used panel (guaranteed non-empty via derived fields) ─
        if pi_ctx_fields:
            evidence["pi_context_panel"] = {
                "process_step": pi_ctx_fields["process_step"],
                "deviation_point": pi_ctx_fields["deviation_point"],
                "variant_path": pi_ctx_fields["variant_path"],
                "cycle_time": pi_ctx_fields["cycle_time"],
                "vendor_behavior": pi_ctx_fields["vendor_behavior"],
                "source_system": "Celonis Event Log (live)",
            }

        # Global metrics
        metrics = []
        if avg_e2e is not None:
            status = "critical" if float(avg_e2e) > 20 else "above_benchmark" if float(avg_e2e) > 15 else "normal"
            metrics.append({"label": "Avg Cycle Time", "value": f"{avg_e2e} days", "benchmark": "<15 days", "status": status})
        if exc_rate is not None:
            status = "critical" if float(exc_rate) > 15 else "above_target" if float(exc_rate) > 5 else "normal"
            metrics.append({"label": "Exception Rate", "value": f"{exc_rate}%", "benchmark": "<3%", "status": status})
        if bottleneck and bottleneck.get("activity"):
            bn_days = bottleneck.get("duration_days", 0)
            status = "critical" if float(bn_days) > 10 else "above_benchmark" if float(bn_days) > 5 else "normal"
            metrics.append({"label": "Bottleneck Duration", "value": f"{bn_days} days", "activity": bottleneck.get("activity", "N/A"), "status": status})
        metrics.append({"label": "Cases Analyzed", "value": str(total_cases), "status": "normal"})
        metrics.append({"label": "Events Processed", "value": str(total_events), "status": "normal"})
        evidence["metrics_used"] = metrics

        if bottleneck and bottleneck.get("activity"):
            pct_of_total = ""
            if avg_e2e and float(avg_e2e) > 0:
                pct_of_total = f"{round(float(bottleneck.get('duration_days', 0)) / float(avg_e2e) * 100)}%"
            evidence["bottleneck_stage"] = {
                "activity": bottleneck.get("activity", "N/A"),
                "avg_days": bottleneck.get("duration_days", 0),
                "case_count": bottleneck.get("case_count", "N/A"),
                "pct_of_total": pct_of_total,
            }

        if graph_path:
            evidence["process_path_detected"] = graph_path
            evidence["path_label"] = path_label
            if process_ctx:
                evidence["golden_path_percentage"] = process_ctx.get("golden_path_percentage", 0)

        exc_patterns = glob.get("exception_patterns", [])
        if exc_patterns:
            evidence["exception_patterns"] = [
                {
                    "type": p.get("exception_type", "N/A"),
                    "frequency_pct": p.get("frequency_percentage", 0),
                    "case_count": p.get("case_count", 0),
                    "avg_resolution_days": p.get("avg_resolution_time_days", "N/A"),
                }
                for p in exc_patterns[:5]
            ]

        variants = glob.get("variants", [])
        if variants:
            evidence["top_variants"] = [
                {"path": v.get("variant", "N/A"), "frequency": v.get("frequency", 0), "percentage": v.get("percentage", 0)}
                for v in variants[:3]
            ]

        trans_dur = glob.get("transition_durations", {})
        if trans_dur:
            evidence["top_transitions"] = [
                {"transition": t, "avg_days": d}
                for t, d in list(trans_dur.items())[:10]
            ]

        confidence = "high" if total_cases >= 100 else "medium" if total_cases >= 20 else "low"
        note = (
            f"Small sample ({total_cases} cases)" if total_cases < 20
            else f"Moderate sample ({total_cases} cases)" if total_cases < 100
            else f"Strong sample ({total_cases} cases) -- statistically significant"
        )
        evidence["data_completeness"] = {
            "cases_analyzed": total_cases,
            "events_analyzed": total_events,
            "confidence": confidence,
            "note": note,
        }

        if event_log is not None and not event_log.empty and "timestamp" in event_log.columns:
            try:
                min_ts = event_log["timestamp"].min()
                max_ts = event_log["timestamp"].max()
                if pd.notnull(min_ts) and pd.notnull(max_ts):
                    evidence["event_log_timespan"] = {
                        "from": str(min_ts.date()) if hasattr(min_ts, "date") else str(min_ts)[:10],
                        "to": str(max_ts.date()) if hasattr(max_ts, "date") else str(max_ts)[:10],
                    }
            except Exception:
                pass

        case_ctx_global = context_used.get("case")
        if case_ctx_global:
            evidence["case_detail"] = {
                "case_id": case_ctx_global.get("case_id", ""),
                "current_stage": case_ctx_global.get("current_stage", "N/A"),
                "days_in_process": case_ctx_global.get("days_in_process", "N/A"),
                "activity_count": case_ctx_global.get("activity_count", 0),
                "variant": case_ctx_global.get("variant", "N/A"),
            }

        if glob.get("conformance_count"):
            evidence["conformance_violations_count"] = glob["conformance_count"]

        # Scope-specific metric overrides
        vendor_ctx = context_used.get("vendor")
        case_ctx = context_used.get("case")

        if vendor_ctx:
            vendor_avg = vendor_ctx.get("avg_duration_days")
            vendor_exc = vendor_ctx.get("exception_rate_pct")
            vendor_cases = vendor_ctx.get("total_cases")
            global_avg = vendor_ctx.get("overall_avg_duration_days", 0)
            global_exc = vendor_ctx.get("overall_exception_rate_pct", 0)

            vendor_metrics = []
            if vendor_avg is not None:
                status = "critical" if float(vendor_avg) > 20 else "above_benchmark" if float(vendor_avg) > 15 else "normal"
                vendor_metrics.append({"label": "Avg Cycle Time", "value": f"{vendor_avg} days", "benchmark": f"Global: {global_avg} days", "status": status})
            if vendor_exc is not None:
                status = "critical" if float(vendor_exc) > 15 else "above_target" if float(vendor_exc) > 5 else "normal"
                vendor_metrics.append({"label": "Exception Rate", "value": f"{vendor_exc}%", "benchmark": f"Global: {global_exc}%", "status": status})
            if vendor_cases is not None:
                vendor_metrics.append({"label": "Cases Analyzed", "value": str(vendor_cases), "status": "normal"})
            if vendor_metrics:
                evidence["metrics_used"] = vendor_metrics
            if vendor_cases is not None:
                confidence = "high" if vendor_cases >= 100 else "medium" if vendor_cases >= 20 else "low"
                evidence["data_completeness"] = {
                    "cases_analyzed": vendor_cases,
                    "events_analyzed": vendor_cases,
                    "confidence": confidence,
                    "note": f"Vendor-scoped: {vendor_cases} cases",
                }
            exc_types = vendor_ctx.get("top_exception_types", [])
            if exc_types:
                evidence["exception_patterns"] = [
                    {"type": e.get("exception_type", "N/A"), "frequency_pct": e.get("pct", 0), "case_count": e.get("case_count", 0), "avg_resolution_days": "N/A"}
                    for e in exc_types[:5]
                ]

        elif case_ctx:
            days_in = case_ctx.get("days_in_process")
            global_avg = (context_used.get("global") or {}).get("avg_end_to_end_days", 0)
            is_exc = case_ctx.get("is_exception", False)
            case_metrics = []
            if days_in is not None:
                status = (
                    "critical" if float(days_in) > float(global_avg or 0) * 1.5
                    else "above_benchmark" if float(days_in) > float(global_avg or 0)
                    else "normal"
                )
                case_metrics.append({"label": "Case Cycle Time", "value": f"{round(days_in, 2)} days", "benchmark": f"Global avg: {global_avg} days", "status": status})
            case_metrics.append({"label": "Current Stage", "value": case_ctx.get("current_stage", "N/A"), "status": "critical" if is_exc else "normal"})
            case_metrics.append({"label": "Activities", "value": str(case_ctx.get("activity_count", 0)), "status": "normal"})
            if case_metrics:
                evidence["metrics_used"] = case_metrics
            evidence["data_completeness"] = {
                "cases_analyzed": 1,
                "events_analyzed": case_ctx.get("activity_count", 0),
                "confidence": "high",
                "note": f"Single case trace -- {case_ctx.get('activity_count', 0)} activities",
            }

        return evidence

    # -----------------------------------------------------------------------
    # Prompt helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _build_user_prompt(message: str, history: List[Dict[str, str]]) -> str:
        LIST_TRIGGERS = [
            "list them", "list it", "show them", "show me", "list all",
            "show all", "give me the list", "what are they", "name them"
        ]
        is_list_followup = (
            any(t in message.lower() for t in LIST_TRIGGERS) and len(message.split()) <= 4
        )
        if not history:
            return message

        history_text = "\n".join(
            f"[{turn['role'].upper()}]: {turn['content']}"
            for turn in history[-8:]
        )
        if is_list_followup:
            return (
                f"CONVERSATION HISTORY:\n{history_text}\n\n"
                f"[USER]: {message}\n\n"
                f"INSTRUCTION: List every individual item from the INDIVIDUAL EXCEPTION CASES "
                f"section. Include case ID, exception type, current stage, and ACTIVE/Resolved status. "
                f"Do NOT summarize."
            )
        return f"CONVERSATION HISTORY:\n{history_text}\n\n[USER]: {message}"

    # -----------------------------------------------------------------------
    # Next steps
    # -----------------------------------------------------------------------

    @staticmethod
    def _generate_next_steps(
        message: str,
        context_used: Dict,
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> List[str]:
        steps: List[str] = []
        q = message.lower()

        if any(x in q for x in ["bottleneck", "delay", "slow", "cycle", "time", "wait"]):
            steps.append("Drill into the bottleneck transition in Celonis Process Explorer")
            steps.append("Compare cycle times across variants to isolate the slow path")
        elif any(x in q for x in ["exception", "error", "block", "overdue", "stuck"]):
            steps.append("Filter the event log by exception activity to see all affected cases")
            steps.append("Check conformance violations for cases in the exception path")
        elif any(x in q for x in ["variant", "path", "route", "flow"]):
            steps.append("Compare variant frequencies in Celonis to identify rework loops")
        elif any(x in q for x in ["vendor", "supplier"]):
            steps.append("Cross-reference vendor exception rate with their payment terms")
        elif any(x in q for x in ["conform", "violation"]):
            steps.append("Open the Celonis Conformance Checker for affected cases")

        if case_id:
            steps.append(f"Pull the full event trace for Case {case_id} in Celonis")
            case_ctx = context_used.get("case", {})
            days_in = case_ctx.get("days_in_process")
            global_avg = context_used.get("global", {}).get("avg_end_to_end_days", 0) or 0
            if isinstance(days_in, float) and days_in > float(global_avg) * 1.5:
                steps.append("Case is significantly overdue -- escalate to stage owner")

        if vendor_id:
            vendor_ctx = context_used.get("vendor", {})
            exc_rate = vendor_ctx.get("exception_rate_pct", 0)
            if isinstance(exc_rate, (int, float)) and exc_rate > 30:
                steps.append(
                    f"Vendor {vendor_id} exception rate {exc_rate}% is very high -- "
                    "review their invoice submission process"
                )

        if not steps:
            steps = [
                "Filter the event log by this activity to see similar instances",
                "Compare throughput time for this transition against the global average",
            ]
        return steps[:4]

    # -----------------------------------------------------------------------
    # Error response
    # -----------------------------------------------------------------------

    @staticmethod
    def _error_response(
        error: str,
        scope_label: str,
        agent_used: str,
        data_sources: List[str],
        context_used: Dict,
    ) -> Dict:
        return {
            "success": False,
            "reply": "I'm unable to answer right now -- the AI service returned an error.",
            "suggested_questions": [],
            "data_sources": data_sources,
            "next_steps": [],
            "context_used": context_used,
            "scope_label": scope_label,
            "agent_used": agent_used,
            "error": error,
            "pi_evidence": None,
            "similar_cases": None,
            "vendor_context": None,
            "graph_path": None,
            "path_label": None,
        }