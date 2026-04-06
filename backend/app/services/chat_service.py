"""
app/services/chat_service.py

Key features:
  1. Event log fetched ONCE per request, reused everywhere (no duplicate Celonis calls)
  2. Vendor ID auto-extracted from message text (handles "vendor 7003196321", "LIFNR 1234" etc.)
  3. Vendor snapshot has 4 fallback layers with proper logging at each step
  4. Vendor exception stats computed from ACTUAL vendor events, not global patterns
  5. Known vendor IDs injected into context so LLM can answer "what vendors exist"
  6. When vendor/case not found, context explains WHY with diagnostics
  7. scope_label corrected when entity not found (avoids misleading the LLM)
  8. process_ctx event_log timespan computed from the cached df (no extra API call)
  9. Exception keyword pattern broadened (block, hold, park, reject, etc.)
 10. Case matching adds pure-numeric fuzzy match
 11. Vendor duration_vs_overall_days computed correctly (was hardcoded 0.0)
 12. System prompt gets a DIAGNOSTICS section when data is missing
 13. next_steps / suggestions skipped on error path (saves compute)
 14. All DataFrame operations guard against NaN before str ops
 15. _build_context returns cached event_log so _extract_pi_evidence reuses it
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

# Broad exception keyword set -- covers SAP AP terminology
EXCEPTION_KEYWORDS = [
    "exception", "due date passed", "block", "blocked", "hold",
    "parked", "park", "reject", "returned", "moved out", "stuck",
    "overdue", "error", "failed", "suspend",
]
_EXCEPTION_PATTERN = re.compile(
    "|".join(re.escape(k) for k in EXCEPTION_KEYWORDS), re.IGNORECASE
)

# SAP LIFNR-style vendor IDs: 7-12 digit numbers
_VENDOR_ID_RE = re.compile(
    r'(?:vendor(?:\s+id)?|lifnr|supplier(?:\s+id)?|vendor\s+number)\s*[:\-]?\s*(\d{5,12})'
    r'|(?<!\d)(\d{7,12})(?!\d)',
    re.IGNORECASE,
)

# Invoice / case ID patterns
_CASE_ID_RE = re.compile(
    r'\b(INV[-_]?\d+|CASE[-_]?\d+|[A-Z]{1,4}\d{6,12})\b',
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Scope instruction templates (referenced in chat() method)
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

# ---------------------------------------------------------------------------
# Pure utility functions
# ---------------------------------------------------------------------------

def _user_wants_graph(message: str) -> bool:
    q = message.lower()
    return any(kw in q for kw in GRAPH_TRIGGER_KEYWORDS)


def _extract_vendor_id(message: str) -> Optional[str]:
    """
    Extract a vendor ID from free-form text.
    Handles:
      - "vendor 7003196321"
      - "LIFNR: 7003196321"
      - "it's vendor id 7003196321"
      - bare 7-12 digit number: "7003196321"
    """
    for m in _VENDOR_ID_RE.finditer(message):
        val = m.group(1) or m.group(2)
        if val:
            return val.strip()
    return None


def _extract_case_id(message: str) -> Optional[str]:
    m = _CASE_ID_RE.search(message)
    return m.group(1).upper() if m else None


def _normalise_id(val: Any) -> str:
    """Strip, uppercase, remove leading zeros for fuzzy matching."""
    return str(val).strip().upper()


def _safe_str_col(series: pd.Series) -> pd.Series:
    """Convert a Series to str, replacing NaN with empty string."""
    return series.fillna("").astype(str)


# ---------------------------------------------------------------------------
# Agent router
# ---------------------------------------------------------------------------

def _pick_agent(
    message: str,
    vendor_id: Optional[str],
    case_id: Optional[str],
) -> str:
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
# System prompt template
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
- Process step: [the current or most relevant activity name from the event log]
- Deviation point: [where this case/process diverged from the expected/golden path -- or "None" if on golden path]
- Variant path: [the actual process path taken, e.g. "Received -> Approved -> Exception -> Resolved"]
- Cycle time: [X days vs benchmark Y days -- always compare to global average]
- Vendor behavior: [vendor-specific history or pattern if a vendor is in scope -- otherwise "N/A"]

**Metrics:**
- Top exceptions: [list each type with % and case count, e.g. "Due Date Passed: 35% (12 cases)"]
- Delays: [specific bottleneck transition and days, e.g. "Approval -> Posting: 8.3 days avg"]
- Cases affected: [exact count and % of total]

**Risk & Impact:**
- Payment delay risk: [specific -- e.g. "Net 30 breach on 8 cases if not resolved by Friday"]
- SLA breach: [Yes/No and exactly why -- cite the threshold]
- Manual effort: [estimated rework impact -- e.g. "~4h per AP clerk based on 3-step resolution path"]

**Next Best Action:**
- [Specific action -- not generic. E.g. "Release block on Case INV-4421 in SAP VIM"]
- [Who should do it -- e.g. "AP Supervisor (role: APSUPR)"]
- [By when -- e.g. "Within 24h to avoid SLA breach on 6 cases"]

**Similar Cases:**
- [List case IDs that had the same issue, how they resolved, and how long it took. If none found: "No similar resolved cases in current dataset."]

======================================================
RULES
======================================================
1.  Use exact numbers. Write "23.94 days" not "about 24 days". Write "35 cases (100%)" not "all cases".
2.  Bold the 2-3 most critical metrics inline within their sections.
3.  Every claim must cite a specific number from the PI data. No unsupported statements.
4.  If data is missing for a field: write "[Data unavailable -- reason]" but complete all other fields.
5.  Compare to global averages whenever possible ("this vendor's 33% vs global 5.7%").
6.  Reference specific activity names and process paths from the event log.
7.  For "active" / "current" / "today" exceptions: use the INDIVIDUAL EXCEPTION CASES section.
    An exception is "active" if the case's last recorded activity is still an exception step.
8.  Answer ONLY from the data below. Never invent numbers.
9.  When VENDOR SCOPE is active: your ENTIRE answer must be about that vendor.
    If the vendor snapshot says "NOT FOUND" -- say so clearly and list which vendor IDs DO exist.
10. When CASE SCOPE is active: your ENTIRE answer must be about that case.
    If the case was not found -- say so and show the sample IDs from the data.
11. When NO scope is active: answer from global process data.
12. Do not rename or reorder the sections above. Follow the format exactly.

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

        # Auto-detect IDs from message if not supplied explicitly
        if not vendor_id:
            vendor_id = _extract_vendor_id(message)
            if vendor_id:
                logger.info("Auto-detected vendor_id=%s from message text", vendor_id)
        if not case_id:
            case_id = _extract_case_id(message)
            if case_id:
                logger.info("Auto-detected case_id=%s from message text", case_id)

        # Fetch event log ONCE -- passed to all builders so no duplicate calls
        event_log = self._fetch_event_log_once()

        # Build all context
        pi_context, context_used, data_sources, process_ctx = self._build_context(
            case_id=case_id,
            vendor_id=vendor_id,
            event_log=event_log,
        )

        # Scope label and LLM instruction
        vendor_found = context_used.get("vendor") is not None
        case_found = context_used.get("case") is not None

        if vendor_id and case_id:
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

        # Graph gate
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

        # Build prompts
        system_prompt = _BASE_SYSTEM_PROMPT.format(
            pi_context=pi_context or "[No event log data available -- check Celonis connection]",
            scope_label=scope_label,
            scope_instruction=scope_instruction,
        )
        user_prompt = self._build_user_prompt(message, conversation_history)

        # LLM call
        try:
            reply = self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
                max_tokens=2500,
            )
        except Exception as exc:
            logger.error("LLM call failed: %s", str(exc))
            return self._error_response(str(exc), scope_label, agent_used, data_sources, context_used)

        # Post-LLM enrichment
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
    # Event log -- single fetch, return empty df on failure
    # -----------------------------------------------------------------------

    def _fetch_event_log_once(self) -> pd.DataFrame:
        try:
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
    ) -> Tuple[str, Dict[str, Any], List[str], Optional[Dict[str, Any]]]:

        context_used: Dict[str, Any] = {}
        sections: List[str] = []
        data_sources: List[str] = []
        process_ctx: Optional[Dict] = None

        # 1. Global process statistics
        try:
            process_ctx = self.process_insight.build_process_context()
            bottleneck = process_ctx.get("bottleneck", {})
            variants = process_ctx.get("variants", [])
            exc_patterns = process_ctx.get("exception_patterns", [])
            conformance = process_ctx.get("conformance_violations", [])
            dec_rules = process_ctx.get("decision_rules", [])
            total_cases = process_ctx.get("total_cases", 0)
            total_events = process_ctx.get("total_events", 0)

            global_section = (
                f"GLOBAL PROCESS STATS  [Celonis Event Log -- {total_events} events, {total_cases} cases]\n"
                f"  Avg end-to-end (days)    : {process_ctx.get('avg_end_to_end_days', 'N/A')}\n"
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

            sections.append(global_section)
            context_used["global"] = {
                "total_cases": total_cases,
                "total_events": total_events,
                "avg_end_to_end_days": process_ctx.get("avg_end_to_end_days"),
                "exception_rate": process_ctx.get("exception_rate"),
                "bottleneck": bottleneck,
                "conformance_count": len(conformance),
                "exception_type_count": len(exc_patterns),
                "exception_patterns": exc_patterns,
                "variants": variants[:5],
            }

            data_sources.append(f"Celonis Event Log -- {total_cases} cases, {total_events} events")
            if variants:
                data_sources.append(f"Process Variants -- {len(variants)} paths mined")
            if exc_patterns:
                data_sources.append(f"Exception Patterns -- {len(exc_patterns)} types")
            if conformance:
                data_sources.append(f"Conformance -- {len(conformance)} violations")

        except Exception as exc:
            logger.warning("Global process context failed: %s", exc)
            sections.append("GLOBAL PROCESS STATS\n  [Unavailable -- check ProcessInsightService]\n")

        # 1b. Individual exception cases (from cached event log)
        if not event_log.empty:
            try:
                exc_section, exc_list = self._build_exception_case_details(event_log)
                if exc_section:
                    sections.append(exc_section)
                    context_used["exception_cases"] = exc_list
                    data_sources.append(f"Exception Case Details -- {len(exc_list)} individual cases")
            except Exception as exc:
                logger.warning("Exception case details failed: %s", exc)

        # 1c. Known vendor IDs (critical for vendor queries)
        known_vendor_ids = self._get_known_vendor_ids(event_log)
        if known_vendor_ids:
            vendor_id_section = (
                f"KNOWN VENDOR IDs IN DATA ({len(known_vendor_ids)} vendors):\n"
                + "".join(f"  - {v}\n" for v in known_vendor_ids[:40])
            )
            if len(known_vendor_ids) > 40:
                vendor_id_section += f"  ... and {len(known_vendor_ids) - 40} more\n"
            sections.append(vendor_id_section)
            context_used["known_vendor_ids"] = known_vendor_ids

        # 2. Case-specific context
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

        # 3. Vendor-specific context
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

        # Strategy 1: exact
        mask = norm_col == lookup
        if mask.any():
            case_events = event_log[mask]
            match_strategy = "exact"

        # Strategy 2: contains
        if case_events.empty:
            mask = norm_col.str.contains(lookup, na=False, regex=False)
            if mask.any():
                case_events = event_log[mask]
                match_strategy = f"contains({lookup})"

        # Strategy 3: core V-number
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

        # Strategy 4: strip leading V
        if case_events.empty:
            stripped = lookup.lstrip("V")
            mask = norm_col == stripped
            if mask.any():
                case_events = event_log[mask]
                match_strategy = f"stripped_V({stripped})"

        # Strategy 5: suffix match (last 8 chars)
        if case_events.empty:
            suffix = lookup[-8:]
            mask = norm_col.str.endswith(suffix, na=False)
            if mask.any():
                case_events = event_log[mask]
                match_strategy = f"suffix({suffix})"

        # Strategy 6: pure-numeric comparison
        if case_events.empty:
            digits_lookup = re.sub(r'\D', '', lookup)
            if digits_lookup:
                norm_digits = norm_col.apply(lambda v: re.sub(r'\D', '', v))
                mask = norm_digits == digits_lookup
                if mask.any():
                    case_events = event_log[mask]
                    match_strategy = f"numeric({digits_lookup})"

        sample_ids = norm_col.unique()[:10].tolist()

        if case_events.empty:
            section = (
                f"CASE DETAIL: {case_id}\n"
                f"  NOT FOUND in the Celonis event log.\n"
                f"  Total distinct cases in log : {event_log['case_id'].nunique()}\n"
                f"  Sample case IDs             : {sample_ids}\n"
                f"  Strategies attempted        : exact, contains, core_V, strip_V, suffix, numeric\n"
                f"  Tip: check case ID format -- leading zeros, prefix letters, or spaces can cause mismatches.\n"
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
        exc_flag = " CURRENTLY IN EXCEPTION" if is_exception else ""

        similar = self._find_similar_cases(event_log, case_id, case_variant)

        section = (
            f"CASE DETAIL: {case_id}  [match: {match_strategy}]\n"
            f"  Current stage      : {current}{exc_flag}\n"
            f"  Days in process    : {round(days_in, 2) if days_in is not None else 'N/A'}{comparison}\n"
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
            dur_diff = round(
                float(snapshot.get("avg_duration_days", 0) or 0) - float(global_days), 2
            )
            dur_sign = "+" if dur_diff > 0 else ""
            exc_diff = round(
                float(snapshot.get("exception_rate_pct", 0) or 0) - float(global_exc), 2
            )
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
                f"    1. The vendor_id column is not joined into the event log extract.\n"
                f"    2. ID format mismatch (leading zeros, case, spaces).\n"
                f"       Try zero-padded: {vendor_id.zfill(10)} or stripped: {vendor_id.lstrip('0')}\n"
                f"    3. This vendor has no invoices in the current dataset.\n"
                f"  Known vendor IDs in data (sample): {sample_vids}\n"
            )

        return section, snapshot

    def _build_vendor_snapshot(
        self,
        vendor_id: str,
        event_log: pd.DataFrame,
        process_ctx: Optional[Dict],
    ) -> Optional[Dict]:
        lookup = vendor_id.strip().upper()

        # L1: pre-built stats
        vendor_stats = process_ctx.get("vendor_stats", []) if process_ctx else []
        row = next(
            (v for v in vendor_stats if _normalise_id(v.get("vendor_id", "")) == lookup),
            None,
        )
        if row:
            logger.info("Vendor %s found in process_ctx.vendor_stats", vendor_id)
            return self._enrich_vendor_row(dict(row), event_log, process_ctx)

        # L2 & L3: search event logs
        vendor_df = self._find_vendor_in_logs(lookup, event_log)
        if vendor_df is None or vendor_df.empty:
            logger.warning(
                "Vendor %s not found in any event log. Known vendors (sample): %s",
                vendor_id,
                self._get_known_vendor_ids(event_log)[:5],
            )
            return None

        logger.info("Vendor %s found -- %d rows", vendor_id, len(vendor_df))

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

    def _find_vendor_in_logs(
        self,
        lookup: str,
        event_log: pd.DataFrame,
    ) -> Optional[pd.DataFrame]:
        enriched: Optional[pd.DataFrame] = None
        try:
            enriched = self.celonis.get_event_log_with_vendor()
            if enriched is None or enriched.empty:
                enriched = None
        except Exception as exc:
            logger.warning("get_event_log_with_vendor failed: %s", exc)
            enriched = None

        candidates = []
        if enriched is not None and "vendor_id" in enriched.columns:
            candidates.append(enriched)
        if "vendor_id" in event_log.columns:
            candidates.append(event_log)

        for df in candidates:
            norm = _safe_str_col(df["vendor_id"]).str.strip().str.upper()

            mask = norm == lookup
            if mask.any():
                return df[mask].copy()

            stripped = lookup.lstrip("0")
            if stripped and stripped != lookup:
                mask = norm == stripped
                if mask.any():
                    return df[mask].copy()

            padded = lookup.zfill(10)
            if padded != lookup:
                mask = norm == padded
                if mask.any():
                    return df[mask].copy()

        return None

    @staticmethod
    def _enrich_vendor_row(
        row: Dict,
        event_log: pd.DataFrame,
        process_ctx: Optional[Dict],
    ) -> Dict:
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
                    _safe_str_col(df["vendor_id"])
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .unique()
                    .tolist()
                )
                ids = [v for v in ids if v.lower() not in ("nan", "none", "")]
                if ids:
                    return sorted(ids)
        try:
            enriched = self.celonis.get_event_log_with_vendor()
            if enriched is not None and not enriched.empty and "vendor_id" in enriched.columns:
                ids = (
                    _safe_str_col(enriched["vendor_id"])
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .unique()
                    .tolist()
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
            details.append({
                "case_id": str(cid),
                "exception_type": str(exc_row["activity"]),
                "exception_time": exc_row["timestamp"],
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
            exc_ts = d["exception_time"].strftime("%Y-%m-%d %H:%M") if pd.notnull(d["exception_time"]) else "N/A"
            last_ts = d["last_activity_time"].strftime("%Y-%m-%d %H:%M") if pd.notnull(d["last_activity_time"]) else "N/A"
            section += (
                f"  - {d['case_id']}: {d['exception_type']} "
                f"(at {exc_ts}) -> now: {d['current_stage']} ({last_ts}) [{status}]\n"
            )

        ctx_list = [
            {
                "case_id": d["case_id"],
                "exception_type": d["exception_type"],
                "exception_time": str(d["exception_time"]) if pd.notnull(d["exception_time"]) else None,
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

        # PRIORITY 1: Case-specific path
        c_ctx = context_used.get("case", {})
        if c_ctx and c_ctx.get("variant"):
            case_id = c_ctx.get("case_id", "")
            days = c_ctx.get("days_in_process")
            days_str = f" - {round(days, 1)}d" if days else ""
            stage = c_ctx.get("current_stage", "")
            exc_flag = " EXCEPTION" if c_ctx.get("is_exception") else ""
            return (
                c_ctx["variant"],
                f"Case {case_id}{days_str} - {stage}{exc_flag}",
            )

        # PRIORITY 2: Vendor-specific path
        if vendor_id:
            v_ctx = context_used.get("vendor", {})
            v_variant = v_ctx.get("most_common_variant", "")
            if v_variant:
                return v_variant, f"Vendor {vendor_id} - most common path"
            if golden:
                return golden, f"Global most common path - {golden_pct}% of cases"
            return None, None

        # PRIORITY 3: Exception / rework path
        if any(x in q for x in ["exception", "rework", "block", "error", "stuck", "due date", "overdue"]):
            for v in variants:
                path = v.get("variant", "")
                if _EXCEPTION_PATTERN.search(path):
                    return path, f"Exception path - {v.get('frequency', '?')} cases ({v.get('percentage', '?')}%)"
            if golden:
                return golden, f"Golden path - {golden_pct}% of cases (no exception variant in top 5)"
            return None, None

        # PRIORITY 4: Bottleneck / delay path
        if any(x in q for x in ["bottleneck", "delay", "slow", "wait"]):
            if bn_activity:
                for v in variants:
                    if bn_activity.lower() in v.get("variant", "").lower():
                        return v["variant"], f"Bottleneck path through '{bn_activity}' - {v.get('frequency', '?')} cases"
            if golden:
                return golden, f"Most common path - {golden_pct}% of cases (bottleneck: {bn_activity})"
            return None, None

        # PRIORITY 5: Conformance path
        if any(x in q for x in ["conform", "violation", "breach", "rule"]):
            for v in variants:
                path = v.get("variant", "")
                if "violation" in path.lower() or "bypass" in path.lower():
                    return path, f"Non-conformant path - {v.get('frequency', '?')} cases"
            if golden:
                return golden, f"Standard conformant path - {golden_pct}% of cases"
            return None, None

        # PRIORITY 6: Default -- golden path
        if golden:
            return golden, f"Golden path - {golden_pct}% of cases"
        return None, None

    # -----------------------------------------------------------------------
    # PI Evidence extraction
    # -----------------------------------------------------------------------

    def _extract_pi_evidence(
        self,
        context_used: Dict,
        process_ctx: Optional[Dict],
        event_log: pd.DataFrame,
        graph_path: Optional[str] = None,
        path_label: Optional[str] = None,
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

        # Global metrics (default)
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

        # Bottleneck
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

        # Graph path
        if graph_path:
            evidence["process_path_detected"] = graph_path
            evidence["path_label"] = path_label
            if process_ctx:
                evidence["golden_path_percentage"] = process_ctx.get("golden_path_percentage", 0)

        # Exception patterns
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

        # Top variants
        variants = glob.get("variants", [])
        if variants:
            evidence["top_variants"] = [
                {"path": v.get("variant", "N/A"), "frequency": v.get("frequency", 0), "percentage": v.get("percentage", 0)}
                for v in variants[:3]
            ]

        # Global data completeness
        confidence = "high" if total_cases >= 100 else "medium" if total_cases >= 20 else "low"
        note = (
            f"Small sample ({total_cases} cases) -- patterns may shift with more data" if total_cases < 20
            else f"Moderate sample ({total_cases} cases) -- directionally reliable" if total_cases < 100
            else f"Strong sample ({total_cases} cases) -- statistically significant"
        )
        evidence["data_completeness"] = {
            "cases_analyzed": total_cases,
            "events_analyzed": total_events,
            "confidence": confidence,
            "note": note,
        }

        # Event log timespan
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

        # Case detail
        case_ctx_global = context_used.get("case")
        if case_ctx_global:
            evidence["case_detail"] = {
                "case_id": case_ctx_global.get("case_id", ""),
                "current_stage": case_ctx_global.get("current_stage", "N/A"),
                "days_in_process": case_ctx_global.get("days_in_process", "N/A"),
                "activity_count": case_ctx_global.get("activity_count", 0),
                "variant": case_ctx_global.get("variant", "N/A"),
            }

        # Conformance
        if glob.get("conformance_count"):
            evidence["conformance_violations_count"] = glob["conformance_count"]

        # Scope-specific metric override
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
                vendor_metrics.append({
                    "label": "Avg Cycle Time",
                    "value": f"{vendor_avg} days",
                    "benchmark": f"Global: {global_avg} days",
                    "status": status,
                })
            if vendor_exc is not None:
                status = "critical" if float(vendor_exc) > 15 else "above_target" if float(vendor_exc) > 5 else "normal"
                vendor_metrics.append({
                    "label": "Exception Rate",
                    "value": f"{vendor_exc}%",
                    "benchmark": f"Global: {global_exc}%",
                    "status": status,
                })
            if vendor_cases is not None:
                vendor_metrics.append({
                    "label": "Cases Analyzed",
                    "value": str(vendor_cases),
                    "status": "normal",
                })
            if vendor_metrics:
                evidence["metrics_used"] = vendor_metrics

            if vendor_cases is not None:
                confidence = "high" if vendor_cases >= 100 else "medium" if vendor_cases >= 20 else "low"
                evidence["data_completeness"] = {
                    "cases_analyzed": vendor_cases,
                    "events_analyzed": vendor_cases,
                    "confidence": confidence,
                    "note": f"Vendor-scoped: {vendor_cases} cases for this vendor only",
                }

            exc_types = vendor_ctx.get("top_exception_types", [])
            if exc_types:
                evidence["exception_patterns"] = [
                    {
                        "type": e.get("exception_type", "N/A"),
                        "frequency_pct": e.get("pct", 0),
                        "case_count": e.get("case_count", 0),
                        "avg_resolution_days": "N/A",
                    }
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
                case_metrics.append({
                    "label": "Case Cycle Time",
                    "value": f"{round(days_in, 2)} days",
                    "benchmark": f"Global avg: {global_avg} days",
                    "status": status,
                })
            case_metrics.append({
                "label": "Current Stage",
                "value": case_ctx.get("current_stage", "N/A"),
                "status": "critical" if is_exc else "normal",
            })
            case_metrics.append({
                "label": "Activities",
                "value": str(case_ctx.get("activity_count", 0)),
                "status": "normal",
            })
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
                f"INSTRUCTION: The user wants you to list the individual items from your previous answer. "
                f"Use the INDIVIDUAL EXCEPTION CASES section in the data to list each case ID, "
                f"its exception type, current stage, and whether it is ACTIVE or Resolved. "
                f"Do NOT summarize -- list every case individually with its details."
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
    # Error response helper
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