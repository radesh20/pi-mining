"""
app/services/chat_service.py
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

# ── Agent routing ─────────────────────────────────────────────────────────────

def _pick_agent(message: str, vendor_id: Optional[str], case_id: Optional[str]) -> str:
    q = message.lower()
    if vendor_id:
        return "Vendor Intelligence Agent"
    if case_id:
        return "Invoice Processing Agent"
    if any(x in q for x in ["exception", "error", "block", "stuck", "issue"]):
        return "Exception Detection Agent"
    if any(x in q for x in ["vendor", "supplier", "lifnr"]):
        return "Vendor Intelligence Agent"
    if any(x in q for x in ["conform", "violation", "rule", "breach"]):
        return "Conformance Checker Agent"
    if any(x in q for x in ["bottleneck", "delay", "slow", "cycle", "time", "duration"]):
        return "Process Insight Agent"
    if any(x in q for x in ["recommend", "next", "action", "what should", "suggest"]):
        return "Case Resolution Agent"
    return "Process Intelligence Agent"


# ── System prompt ─────────────────────────────────────────────────────────────

BASE_SYSTEM_PROMPT = """You are a Process Intelligence Assistant for an AP invoice management system.
You speak to operations managers — not developers. Write like a sharp analyst, not a chatbot.

OUTPUT RULES:
1. Answer in the FIRST sentence. No preamble like "Based on the data..." or "According to Celonis..."
2. Use exact numbers. Write "23.94 days" not "about 24 days". Write "35 cases (100%)" not "all cases".
3. Bold the single most important fact per answer using **bold**. Max 2 bolds per reply.
4. Use flat bullet points for lists. Never nest bullets.
5. Never write section headers like "Overview:" or numbered sections like "1. Exception Details".
6. End with one crisp action line starting with "→ Recommended action:" when relevant.
7. Max 5 bullets OR 3 short paragraphs. No essays.
8. If data is missing: say "Not available in the Celonis event log."
9. For "active", "current", or "today" exception queries: use the INDIVIDUAL EXCEPTION CASES section.
   An exception is "active" if the case's last recorded activity is still an exception step (not yet resolved).
   Use the timestamps provided to identify recent exceptions.

DATA RULES:
- Answer ONLY from the PI data below. Never invent numbers.
- When VENDOR SCOPE is active: your entire answer must be about that specific vendor.
  Compare their stats to the global averages provided. Do NOT give a general process answer.
- When CASE SCOPE is active: your entire answer must be about that specific invoice case.
  Reference exact stage, days, and activity path. Do NOT give a general process answer.
- When no scope: answer from global process data only.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LIVE CELONIS EVENT LOG DATA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{pi_context}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ACTIVE SCOPE: {scope_label}
{scope_instruction}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

VENDOR_SCOPE_INSTRUCTION = """
⚠ VENDOR SCOPE ACTIVE — Vendor {vendor_id}
Your answer MUST be about this vendor only.
Use the VENDOR SNAPSHOT section. Compare their exception rate and cycle time to the global averages shown.
Do not answer about the overall process. The user wants to know about THIS vendor.
"""

CASE_SCOPE_INSTRUCTION = """
⚠ CASE SCOPE ACTIVE — Case {case_id}
Your answer MUST be about this invoice case only.
Use the CASE DETAIL section. Reference the exact current stage, days in process, and activity path.
Do not answer about the overall process. The user wants to know about THIS invoice.
"""


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

    # ── Public entry point ───────────────────────────────────────────────────

    def chat(
        self,
        message: str,
        conversation_history: List[Dict[str, str]],
        case_id: Optional[str] = None,
        vendor_id: Optional[str] = None,
    ) -> Dict[str, Any]:

        pi_context, context_used, data_sources = self._build_context(
            case_id=case_id, vendor_id=vendor_id
        )

        if vendor_id and case_id:
            scope_label = f"Vendor {vendor_id} + Case {case_id}"
            scope_instruction = (
                VENDOR_SCOPE_INSTRUCTION.format(vendor_id=vendor_id) +
                CASE_SCOPE_INSTRUCTION.format(case_id=case_id)
            )
        elif vendor_id:
            scope_label = f"Vendor {vendor_id} only"
            scope_instruction = VENDOR_SCOPE_INSTRUCTION.format(vendor_id=vendor_id)
        elif case_id:
            scope_label = f"Case {case_id} only"
            scope_instruction = CASE_SCOPE_INSTRUCTION.format(case_id=case_id)
        else:
            scope_label = "Global — all cases and vendors"
            scope_instruction = ""

        agent_used = _pick_agent(message, vendor_id, case_id)

        system_prompt = BASE_SYSTEM_PROMPT.format(
            pi_context=pi_context,
            scope_label=scope_label,
            scope_instruction=scope_instruction,
        )
        user_prompt = self._build_user_prompt(message, conversation_history)

        try:
            reply = self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
                max_tokens=2500,
            )

            suggested_questions = []
            try:
                suggested_questions = self.suggestion_service.generate_suggestions(
                    user_message=message,
                    ai_reply=reply,
                    context_used=context_used,
                    case_id=case_id,
                    vendor_id=vendor_id,
                )
            except Exception as e:
                logger.warning("Suggestion generation failed: %s", str(e))

            next_steps = self._generate_next_steps(
                message=message,
                context_used=context_used,
                case_id=case_id,
                vendor_id=vendor_id,
            )

            return {
                "success":             True,
                "reply":               reply,
                "suggested_questions": suggested_questions,
                "data_sources":        data_sources,
                "next_steps":          next_steps,
                "context_used":        context_used,
                "scope_label":         scope_label,
                "agent_used":          agent_used,
                "error":               None,
            }

        except Exception as exc:
            logger.error("ChatService LLM call failed: %s", str(exc))
            return {
                "success":             False,
                "reply":               "I'm unable to answer right now — the AI service returned an error.",
                "suggested_questions": [],
                "data_sources":        data_sources,
                "next_steps":          [],
                "context_used":        context_used,
                "scope_label":         scope_label,
                "agent_used":          agent_used,
                "error":               str(exc),
            }

    # ── Context builder ──────────────────────────────────────────────────────

    def _build_context(
        self,
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> Tuple[str, Dict[str, Any], List[str]]:

        context_used: Dict[str, Any] = {}
        sections: List[str] = []
        data_sources: List[str] = []
        process_ctx = None

        # ── 1. Global PI snapshot ────────────────────────────────────────────
        try:
            process_ctx   = self.process_insight.build_process_context()
            bottleneck    = process_ctx.get("bottleneck", {})
            variants      = process_ctx.get("variants", [])
            exc_patterns  = process_ctx.get("exception_patterns", [])
            conformance   = process_ctx.get("conformance_violations", [])
            decision_rules = process_ctx.get("decision_rules", [])
            total_cases   = process_ctx.get("total_cases", 0)
            total_events  = process_ctx.get("total_events", 0)

            global_section = (
                f"GLOBAL PROCESS STATS  [Source: Celonis Event Log — {total_events} events]\n"
                f"  Total cases              : {total_cases}\n"
                f"  Total events             : {total_events}\n"
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
                    global_section += f"    • {v['variant']} ({v['frequency']} cases, {v['percentage']}%)\n"

            if exc_patterns:
                global_section += f"  Exception patterns ({len(exc_patterns)} types):\n"
                for p in exc_patterns:
                    global_section += (
                        f"    • {p['exception_type']}: {p['frequency_percentage']}% of cases, "
                        f"{p['case_count']} cases, avg resolution {p['avg_resolution_time_days']} days, "
                        f"trigger: {p['trigger_condition']}, "
                        f"resolved by: {p['typical_resolution']} (role: {p['resolution_role']})\n"
                    )
            else:
                global_section += "  Exception patterns: None detected\n"

            if conformance:
                global_section += f"  Conformance violations ({len(conformance)} found):\n"
                for v in conformance:
                    global_section += (
                        f"    • VIOLATION: {v['rule']}\n"
                        f"      Rate: {v['violation_rate']}% ({v['affected_cases']} of {v['total_cases']} cases)\n"
                        f"      Description: {v['violation_description']}\n"
                    )
            else:
                global_section += "  Conformance violations: None\n"

            if decision_rules:
                global_section += "  Decision rules:\n"
                for r in decision_rules:
                    global_section += f"    • {r['condition']} → {r['action']} (confidence: {r['confidence']})\n"

            sections.append(global_section)
            context_used["global"] = {
                "total_cases":          total_cases,
                "total_events":         total_events,
                "avg_end_to_end_days":  process_ctx.get("avg_end_to_end_days"),
                "exception_rate":       process_ctx.get("exception_rate"),
                "bottleneck":           bottleneck,
                "conformance_count":    len(conformance),
                "exception_type_count": len(exc_patterns),
                "exception_patterns":   exc_patterns,
                "variants":             variants[:5],
            }

            data_sources.append(
                f"Celonis Event Log — {total_cases} cases, {total_events} events"
            )
            if variants:
                data_sources.append(
                    f"Process Variants — {len(variants)} distinct paths mined"
                )
            if exc_patterns:
                data_sources.append(
                    f"Exception Patterns — {len(exc_patterns)} types detected from event log"
                )
            if conformance:
                data_sources.append(
                    f"Conformance Analysis — {len(conformance)} violations detected"
                )

        except Exception as exc:
            logger.warning("Could not load global process context: %s", str(exc))
            sections.append("GLOBAL PROCESS STATS\n  [Unavailable]\n")

        # ── 1b. Individual exception cases for temporal queries ───────────
        try:
            exc_event_log = self.celonis.get_event_log()
            if not exc_event_log.empty:
                exc_detail_section, exc_detail_context = self._build_exception_case_details(exc_event_log)
                if exc_detail_section:
                    sections.append(exc_detail_section)
                    context_used["exception_cases"] = exc_detail_context
                    data_sources.append(
                        f"Exception Case Details — {len(exc_detail_context)} individual cases with timestamps"
                    )
        except Exception as exc:
            logger.warning("Could not build exception case details: %s", str(exc))

        # ── 2. Case-specific context ─────────────────────────────────────────
        if case_id:
            try:
                event_log = self.celonis.get_event_log()

                # ── Normalized lookup with multiple fallback strategies ──
                lookup_id = str(case_id).strip().upper()
                norm_col  = event_log["case_id"].astype(str).str.strip().str.upper()

                # Strategy 1: exact match
                case_events = event_log[norm_col == lookup_id]
                match_strategy = "exact"

                # Strategy 2: stored ID contains the entered ID anywhere
                # This handles format: 300V41130634800003 containing V411306348
                if case_events.empty:
                    case_events = event_log[norm_col.str.contains(lookup_id, na=False, regex=False)]
                    if not case_events.empty:
                        match_strategy = f"contains ({lookup_id})"

                # Strategy 3: extract V+digits core from stored ID and compare
                if case_events.empty:
                    def extract_v_core(val: str) -> str:
                        m = re.search(r'V\d+', str(val))
                        return m.group(0) if m else str(val)
                    norm_core = norm_col.apply(extract_v_core)
                    lookup_core_m = re.search(r'V\d+', lookup_id)
                    lookup_core = lookup_core_m.group(0) if lookup_core_m else lookup_id
                    case_events = event_log[norm_core == lookup_core]
                    if not case_events.empty:
                        match_strategy = f"core_match ({lookup_core})"

                # Strategy 4: strip leading 'V' from lookup
                if case_events.empty:
                    stripped_v = lookup_id.lstrip("V")
                    case_events = event_log[norm_col == stripped_v]
                    if not case_events.empty:
                        match_strategy = f"stripped_V ({stripped_v})"

                # Strategy 5: suffix match on last 8 characters
                if case_events.empty:
                    suffix = lookup_id[-8:]
                    case_events = event_log[norm_col.str.endswith(suffix, na=False)]
                    if not case_events.empty:
                        match_strategy = f"suffix_match ({suffix})"

                # Log results
                sample_ids = norm_col.unique()[:8].tolist()
                logger.info(
                    "Case lookup: requested='%s' normalized='%s' strategy='%s' "
                    "total_cases=%d matched_rows=%d sample_ids=%s",
                    case_id,
                    lookup_id,
                    match_strategy,
                    event_log["case_id"].nunique(),
                    len(case_events),
                    sample_ids,
                )

                if not case_events.empty:
                    sorted_events   = case_events.sort_values("timestamp")
                    activities      = sorted_events["activity"].tolist()
                    current_stage   = activities[-1] if activities else "Unknown"
                    start_time      = sorted_events["timestamp"].min()
                    last_time       = sorted_events["timestamp"].max()
                    days_in_process = (
                        (last_time - start_time).total_seconds() / 86400
                        if pd.notnull(start_time) and pd.notnull(last_time) else "N/A"
                    )
                    case_variant = " → ".join(activities)

                    global_avg = process_ctx.get("avg_end_to_end_days", 0) if process_ctx else 0
                    comparison = ""
                    if isinstance(days_in_process, float) and global_avg:
                        diff = round(days_in_process - float(global_avg), 1)
                        comparison = f" ({'+' if diff > 0 else ''}{diff}d vs global avg {global_avg}d)"

                    similar_cases = self._find_similar_cases(
                        event_log=event_log,
                        case_id=case_id,
                        case_variant=case_variant,
                    )

                    case_section = (
                        f"CASE DETAIL: {case_id}  [Source: Celonis Event Log]\n"
                        f"  Match strategy     : {match_strategy}\n"
                        f"  Current stage      : {current_stage}\n"
                        f"  Days in process    : {round(days_in_process, 2) if isinstance(days_in_process, float) else days_in_process}{comparison}\n"
                        f"  Total activities   : {len(activities)}\n"
                        f"  Full process path  : {case_variant}\n"
                        f"  Similar cases      : {len(similar_cases)} cases follow the same path\n"
                    )
                    sections.append(case_section)

                    context_used["case"] = {
                        "case_id":         case_id,
                        "current_stage":   current_stage,
                        "days_in_process": days_in_process,
                        "activity_count":  len(activities),
                        "variant":         case_variant,
                        "match_strategy":  match_strategy,
                    }
                    context_used["similar_cases"] = similar_cases

                    data_sources.append(
                        f"Case {case_id} Event Trace — {len(activities)} activities, current: {current_stage}"
                    )
                    if similar_cases:
                        data_sources.append(
                            f"Similar Cases — {len(similar_cases)} cases with matching process path"
                        )

                else:
                    # ── Detailed fallback so the LLM gives a useful response ──
                    logger.warning(
                        "Case '%s' not found in event log after all strategies. "
                        "Sample stored IDs: %s",
                        case_id,
                        sample_ids,
                    )
                    sections.append(
                        f"CASE DETAIL: {case_id}\n"
                        f"  ⚠ Case '{case_id}' was NOT found in the Celonis event log.\n"
                        f"  Total cases in log   : {event_log['case_id'].nunique()}\n"
                        f"  Sample IDs in log    : {sample_ids}\n"
                        f"  Strategies tried     : exact, contains, core_match, strip_V, suffix_match\n"
                        f"  Likely cause         : Case ID format mismatch between UI input and "
                        f"the value stored in the Celonis CASE_COLUMN.\n"
                        f"  Suggested action     : Check sample IDs above and match the format "
                        f"when entering a case ID in the UI.\n"
                    )

            except Exception as exc:
                logger.warning("Could not load case context for %s: %s", case_id, str(exc))

        # ── 3. Vendor-specific context ───────────────────────────────────────
        if vendor_id:
            try:
                vendor_snapshot = self._build_vendor_snapshot(vendor_id, process_ctx)

                if vendor_snapshot:
                    vs = vendor_snapshot
                    global_exc   = vs.get("overall_exception_rate_pct", 0) or 0
                    global_days  = vs.get("overall_avg_duration_days", 0) or 0

                    vendor_section = (
                        f"VENDOR SNAPSHOT: {vendor_id}  [Source: Celonis Event Log + Vendor Mapping]\n"
                        f"  Total cases          : {vs.get('total_cases', 'N/A')}\n"
                        f"  Exception rate       : {vs.get('exception_rate_pct', 'N/A')}%  "
                        f"(global avg: {global_exc}%)\n"
                        f"  Avg cycle time       : {vs.get('avg_duration_days', 'N/A')} days  "
                        f"(global avg: {global_days} days)\n"
                        f"  vs. global (days)    : {vs.get('duration_vs_overall_days', 'N/A')}\n"
                        f"  Exception cases      : {vs.get('exception_case_count', 'N/A')}\n"
                        f"  Most common variant  : {vs.get('most_common_variant', 'N/A')}\n"
                        f"  Payment terms        : {vs.get('payment_terms', 'N/A')}\n"
                        f"  Currency             : {vs.get('currency', 'N/A')}\n"
                    )
                    top_exc = vs.get("top_exception_types", [])
                    if top_exc:
                        vendor_section += "  Exception types:\n"
                        for e in top_exc:
                            vendor_section += f"    • {e['exception_type']}: {e['case_count']} cases\n"

                    sections.append(vendor_section)
                    context_used["vendor"] = vendor_snapshot

                    data_sources.append(
                        f"Vendor {vendor_id} — {vs.get('total_cases', 0)} cases, "
                        f"{vs.get('exception_rate_pct', 0)}% exception rate"
                    )
                else:
                    sections.append(f"VENDOR SNAPSHOT: {vendor_id}\n  [Not found in event log]\n")

            except Exception as exc:
                logger.warning("Could not load vendor context for %s: %s", vendor_id, str(exc))

        return "\n".join(sections), context_used, data_sources

    # ── Vendor snapshot builder ──────────────────────────────────────────────

    def _build_vendor_snapshot(
        self,
        vendor_id: str,
        process_ctx: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        try:
            vendor_stats_list: List[Dict] = (
                process_ctx.get("vendor_stats", []) if process_ctx else []
            )

            vendor_row = next(
                (v for v in vendor_stats_list
                 if str(v.get("vendor_id", "")).upper() == vendor_id.upper()),
                None,
            )

            if not vendor_row:
                enriched = self.celonis.get_event_log_with_vendor()
                if enriched.empty:
                    return None

                vendor_df = enriched[
                    enriched["vendor_id"].astype(str).str.upper().str.strip()
                    == vendor_id.upper().strip()
                ]
                if vendor_df.empty:
                    return None

                total_cases = int(vendor_df["case_id"].nunique())
                exc_cases   = int(
                    vendor_df[
                        vendor_df["activity"].str.contains("exception", case=False, na=False)
                    ]["case_id"].nunique()
                )
                case_dur = (
                    vendor_df.groupby("case_id")
                    .agg(start=("timestamp", "min"), end=("timestamp", "max"))
                    .reset_index()
                )
                case_dur["dur"] = (
                    (case_dur["end"] - case_dur["start"]).dt.total_seconds() / 86400
                )
                avg_dur = round(float(case_dur["dur"].mean()), 2) if not case_dur.empty else 0.0

                most_common_variant = ""
                try:
                    v_variants = (
                        vendor_df.sort_values(["case_id", "timestamp"])
                        .groupby("case_id")["activity"]
                        .apply(lambda x: " → ".join(x.astype(str).tolist()))
                        .value_counts()
                    )
                    most_common_variant = str(v_variants.index[0]) if not v_variants.empty else ""
                except Exception:
                    pass

                payment_terms = ""
                currency = ""
                try:
                    attrs = vendor_df[["payment_terms", "currency"]].dropna(how="all")
                    if not attrs.empty:
                        payment_terms = str(attrs["payment_terms"].dropna().iloc[0]) if not attrs["payment_terms"].dropna().empty else ""
                        currency      = str(attrs["currency"].dropna().iloc[0]) if not attrs["currency"].dropna().empty else ""
                except Exception:
                    pass

                vendor_row = {
                    "vendor_id":            vendor_id,
                    "total_cases":          total_cases,
                    "exception_case_count": exc_cases,
                    "exception_rate_pct":   round(exc_cases / total_cases * 100, 2) if total_cases else 0.0,
                    "avg_duration_days":    avg_dur,
                    "most_common_variant":  most_common_variant,
                    "duration_vs_overall_days": 0.0,
                    "payment_terms":        payment_terms,
                    "currency":             currency,
                }

            if process_ctx:
                vendor_row["overall_avg_duration_days"]  = process_ctx.get("avg_end_to_end_days", 0.0)
                vendor_row["overall_exception_rate_pct"] = process_ctx.get("exception_rate", 0.0)

            top_exception_types = []
            if process_ctx:
                for p in process_ctx.get("exception_patterns", [])[:3]:
                    top_exception_types.append({
                        "exception_type": p["exception_type"],
                        "case_count":     p["case_count"],
                    })
            vendor_row["top_exception_types"] = top_exception_types

            return vendor_row

        except Exception as exc:
            logger.warning("Vendor snapshot failed for %s: %s", vendor_id, str(exc))
            return None

    # ── Individual exception cases ────────────────────────────────────────────

    def _build_exception_case_details(
        self,
        event_log: pd.DataFrame,
        max_cases: int = 20,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """Build individual exception case details for temporal and active-exception queries."""
        if event_log.empty:
            return "", []

        exc_keywords = ["exception", "due date passed", "block", "moved out"]
        pattern = "|".join(exc_keywords)
        exc_events = event_log[
            event_log["activity"].str.lower().str.contains(pattern, regex=True, na=False)
        ]

        if exc_events.empty:
            return "", []

        exc_case_ids = exc_events["case_id"].unique()
        details: List[Dict[str, Any]] = []

        for cid in exc_case_ids:
            case_events = event_log[event_log["case_id"] == cid].sort_values("timestamp")
            if case_events.empty:
                continue

            case_exc = exc_events[exc_events["case_id"] == cid].sort_values("timestamp")
            exc_row = case_exc.iloc[-1]
            last_row = case_events.iloc[-1]

            last_lower = str(last_row["activity"]).lower()
            is_active = any(kw in last_lower for kw in exc_keywords)

            details.append({
                "case_id": str(cid),
                "exception_type": str(exc_row["activity"]),
                "exception_time": exc_row["timestamp"],
                "current_stage": str(last_row["activity"]),
                "last_activity_time": last_row["timestamp"],
                "is_active": is_active,
                "event_count": len(case_events),
            })

        details.sort(
            key=lambda x: x["exception_time"] if pd.notnull(x["exception_time"]) else pd.Timestamp.min,
            reverse=True,
        )

        active_count = sum(1 for d in details if d["is_active"])
        resolved_count = len(details) - active_count
        show = details[:max_cases]

        section = (
            f"INDIVIDUAL EXCEPTION CASES ({len(details)} total: "
            f"{active_count} active, {resolved_count} resolved)  "
            f"[Source: Celonis Event Log]\n"
        )
        for d in show:
            status = "ACTIVE" if d["is_active"] else "Resolved"
            exc_ts = (
                d["exception_time"].strftime("%Y-%m-%d %H:%M")
                if pd.notnull(d["exception_time"]) else "N/A"
            )
            last_ts = (
                d["last_activity_time"].strftime("%Y-%m-%d %H:%M")
                if pd.notnull(d["last_activity_time"]) else "N/A"
            )
            section += (
                f"  \u2022 Case {d['case_id']}: {d['exception_type']} "
                f"(occurred: {exc_ts}, current: {d['current_stage']} at {last_ts}, "
                f"status: {status})\n"
            )

        context_list = [
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

        return section, context_list

    # ── Similar cases ────────────────────────────────────────────────────────

    def _find_similar_cases(
        self,
        event_log: pd.DataFrame,
        case_id: str,
        case_variant: str,
        max_results: int = 5,
    ) -> List[Dict[str, Any]]:
        try:
            case_variants = (
                event_log.sort_values(["case_id", "timestamp"])
                .groupby("case_id")["activity"]
                .apply(lambda x: " → ".join(x.astype(str).tolist()))
                .reset_index(name="variant")
            )
            same_variant = case_variants[
                (case_variants["variant"] == case_variant) &
                (case_variants["case_id"].astype(str).str.strip().str.upper()
                 != str(case_id).strip().upper())
            ]
            if same_variant.empty:
                return []

            case_durations = (
                event_log.groupby("case_id")
                .agg(start_time=("timestamp", "min"), end_time=("timestamp", "max"))
                .reset_index()
            )
            case_durations["duration_days"] = (
                (case_durations["end_time"] - case_durations["start_time"])
                .dt.total_seconds() / 86400
            ).round(2)

            similar = same_variant.merge(
                case_durations[["case_id", "duration_days"]], on="case_id", how="left"
            )

            result = []
            for _, row in similar.head(max_results).iterrows():
                case_ev = event_log[
                    event_log["case_id"].astype(str).str.strip().str.upper()
                    == str(row["case_id"]).strip().upper()
                ]
                last_activity = (
                    case_ev.sort_values("timestamp").iloc[-1]["activity"]
                    if not case_ev.empty else "Unknown"
                )
                result.append({
                    "case_id":       str(row["case_id"]),
                    "duration_days": round(float(row["duration_days"]), 2) if pd.notnull(row["duration_days"]) else None,
                    "current_stage": last_activity,
                    "variant_match": True,
                })
            return result

        except Exception as exc:
            logger.warning("Similar cases lookup failed: %s", str(exc))
            return []

    # ── Next steps ───────────────────────────────────────────────────────────

    @staticmethod
    def _generate_next_steps(
        message: str,
        context_used: Dict[str, Any],
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> List[str]:
        steps = []
        q = message.lower()

        if any(x in q for x in ["bottleneck", "delay", "slow", "time", "cycle"]):
            steps.append("Drill into the bottleneck transition in Celonis Process Explorer")
            steps.append("Compare cycle times across variants to isolate the slow path")
        elif any(x in q for x in ["exception", "error", "problem", "block"]):
            steps.append("Filter the Celonis event log by exception activity to see all affected cases")
            steps.append("Check conformance violations for cases in the exception path")
        elif any(x in q for x in ["variant", "path", "route", "flow"]):
            steps.append("Compare variant frequencies in Celonis to identify rework loops")
        elif any(x in q for x in ["vendor", "supplier"]):
            steps.append("Cross-reference vendor exception rate with their payment terms in the case table")
        elif any(x in q for x in ["conform", "violation"]):
            steps.append("Open the Celonis Conformance Checker for the affected cases")

        if case_id:
            steps.append(f"Pull the full event trace for Case {case_id} in the Celonis event log")
            case_ctx = context_used.get("case", {})
            if isinstance(case_ctx.get("days_in_process"), float) and case_ctx["days_in_process"] > 10:
                steps.append("Case appears overdue — escalate to the stage owner from resource mapping")

        if vendor_id:
            vendor_ctx = context_used.get("vendor", {})
            exc_rate = vendor_ctx.get("exception_rate_pct", 0)
            if isinstance(exc_rate, (int, float)) and exc_rate > 30:
                steps.append(f"Vendor {vendor_id} exception rate is high — review their invoice submission process")

        if not steps:
            steps = [
                "Filter the Celonis event log by this activity to see similar instances",
                "Compare throughput time for this transition against the global average",
            ]

        return steps[:4]

    # ── Prompt helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _build_user_prompt(message: str, history: List[Dict[str, str]]) -> str:
        if not history:
            return message
        history_text = "\n".join(
            f"[{turn['role'].upper()}]: {turn['content']}" for turn in history[-6:]
        )
        return f"CONVERSATION HISTORY:\n{history_text}\n\n[USER]: {message}"