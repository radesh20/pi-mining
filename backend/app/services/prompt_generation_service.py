import json
from typing import Any, Dict, List
from app.services.azure_openai_service import AzureOpenAIService

DEEP_DIVE_SYSTEM = """
You are an AI agent prompt engineer for enterprise process agents.

You will receive REAL process mining insights from Celonis.
Generate THREE types of prompts for a specific agent:

1. WORKFLOW PROMPTS — Step-by-step instructions from the discovered process model.
   Include TURNAROUND TIME awareness from Celonis Process Explorer data.
   Every step must include [SOURCE: Celonis Process Mining] with the specific metric.

2. DECISION LOGIC — Conditional rules from patterns observed in Celonis event logs.
   NOT generic rules — only rules backed by actual data patterns.

3. GUARDRAILS — Hard constraints derived from:
   - Conformance violations detected in Celonis
   - Resource/role analysis from Celonis event logs
   - Variant frequency analysis (rare variants = escalate)

Return valid JSON:
{
  "agent_name": "...",
  "data_source": "Celonis Process Mining",
  "pi_superiority_summary": "...",
  "workflow_prompts": {
    "system_prompt": "...",
    "steps": [
      {
        "step": 1,
        "prompt": "...",
        "pi_evidence_used": "...",
        "timing_decision": "...",
        "action_impact": "...",
        "why_bi_only_misses_this": "...",
        "source": "Celonis: ..."
      }
    ]
  },
  "decision_logic": [
    {
      "prompt": "...",
      "condition": "...",
      "action": "...",
      "pi_evidence_used": "...",
      "timing_decision": "...",
      "action_impact": "...",
      "why_bi_only_misses_this": "...",
      "confidence": 0.0,
      "source": "Celonis: ..."
    }
  ],
  "guardrails": [
    {
      "type": "compliance|authority|risk|exception|temporal",
      "prompt": "...",
      "rule": "...",
      "pi_evidence_used": "...",
      "timing_decision": "...",
      "action_impact": "...",
      "why_bi_only_misses_this": "...",
      "source": "Celonis: ...",
      "enforcement": "BLOCK|ESCALATE|WARN"
    }
  ],
  "critical_scenario": {
    "scenario": "...",
    "days_until_due": 7,
    "historical_processing_days": 3.2,
    "recommended_action": "...",
    "why_pi_is_better": "..."
  }
}
"""

COMPARISON_SYSTEM = """
You are a process automation expert.
Show the critical difference between agent prompts generated WITH Celonis process mining
vs prompts generated from traditional BI/RDBMS data only.

The key insight: Celonis provides turnaround time awareness, exception resolution patterns,
role-based escalation paths, conformance-derived guardrails, and variant-based automation confidence
that NO BI tool or relational database can provide.

Return valid JSON:
{
  "without_process_mining": {
    "prompt": "...",
    "limitations": ["..."]
  },
  "with_process_mining": {
    "prompt": "...",
    "advantages": ["..."],
    "celonis_unique_insights": ["..."]
  },
  "key_differences": ["..."]
}
"""


class PromptGenerationService:
    def __init__(self, llm: AzureOpenAIService):
        self.llm = llm

    def generate_deep_dive(
        self,
        agent_name: str,
        process_context: dict,
        case_data: Dict[str, Any] | None = None,
        exception_records: List[Dict[str, Any]] | None = None,
    ) -> dict:
        case_data = case_data or {}
        exception_records = exception_records or []
        matching_case_exceptions = [
            e for e in exception_records
            if str(e.get("case_id")) == str(case_data.get("case_id"))
        ]
        safe_context = {
            "activity_durations": process_context.get("activity_durations", {}),
            "avg_end_to_end_days": process_context.get("avg_end_to_end_days", 0),
            "bottleneck": process_context.get("bottleneck", {}),
            "exception_patterns": process_context.get("exception_patterns", []),
            "decision_rules": process_context.get("decision_rules", []),
            "conformance_violations": process_context.get("conformance_violations", []),
            "role_mappings": process_context.get("role_mappings", []),
            "variants": process_context.get("variants", []),
            "total_cases": process_context.get("total_cases", 0),
            "total_events": process_context.get("total_events", 0),
            "exception_rate": process_context.get("exception_rate", 0),
        }
        try:
            user_prompt = f"""
Generate comprehensive prompts for: {agent_name}
Data source: Celonis Process Mining

=== CASE LEVEL PI EVIDENCE ===

Case ID: {case_data.get("case_id")}
Activity Trace: {case_data.get("activity_trace_text", "")}
Actual Duration: {case_data.get("actual_dpo", 0.0)}

Exceptions for this case:
{json.dumps(matching_case_exceptions, indent=2)}

=== END CASE CONTEXT ===

=== CELONIS PROCESS MINING CONTEXT ===

TURNAROUND TIMES (from Celonis Process Explorer):
{json.dumps(safe_context["activity_durations"], indent=2)}

Avg End-to-End: {safe_context["avg_end_to_end_days"]} days
Bottleneck: {json.dumps(safe_context["bottleneck"])}

EXCEPTION PATTERNS (mined from Celonis variants):
{json.dumps(safe_context["exception_patterns"], indent=2)}

DECISION RULES (mined from Celonis event logs):
{json.dumps(safe_context["decision_rules"], indent=2)}

CONFORMANCE VIOLATIONS (from Celonis conformance checking):
{json.dumps(safe_context["conformance_violations"], indent=2)}

ROLE-ACTIVITY MAPPINGS (from Celonis resource analysis):
{json.dumps(safe_context["role_mappings"], indent=2)}

VARIANT FREQUENCIES (from Celonis variant analysis):
{json.dumps(safe_context["variants"][:10], indent=2)}

PROCESS STATS:
Cases: {safe_context["total_cases"]}
Events: {safe_context["total_events"]}
Exception Rate: {safe_context["exception_rate"]}%

=== END CELONIS DATA ===

Generate workflow prompts, decision logic, and guardrails.
EVERY element must reference Celonis as the data source with specific metrics.
"""
            result = self.llm.chat_json(DEEP_DIVE_SYSTEM, user_prompt)
            return self._normalize_deep_dive(result, agent_name, process_context)
        except Exception as e:
            return self._fallback_deep_dive(agent_name, process_context, str(e))

    def generate_comparison(self, agent_name: str, process_context: dict) -> dict:
        safe_context = {
            "activity_durations": process_context.get("activity_durations", {}),
            "bottleneck": process_context.get("bottleneck", {}),
            "exception_patterns": process_context.get("exception_patterns", []),
            "conformance_violations": process_context.get("conformance_violations", []),
            "role_mappings": process_context.get("role_mappings", []),
            "variants": process_context.get("variants", []),
            "avg_end_to_end_days": process_context.get("avg_end_to_end_days", 0),
            "exception_rate": process_context.get("exception_rate", 0),
        }
        user_prompt = f"""
For the agent "{agent_name}", generate a side-by-side comparison:

1. PROMPT WITHOUT PROCESS MINING (what a BI tool / RDBMS would produce)
2. PROMPT WITH CELONIS PROCESS MINING (what Celonis process intelligence provides)

Use this REAL Celonis data to populate the "with process mining" section:

Turnaround Times: {json.dumps(safe_context["activity_durations"])}
Bottleneck: {json.dumps(safe_context["bottleneck"])}
Exception Patterns: {json.dumps(safe_context["exception_patterns"])}
Conformance Violations: {json.dumps(safe_context["conformance_violations"])}
Role Mappings: {json.dumps(safe_context["role_mappings"])}
Variants: {json.dumps(safe_context["variants"][:5])}
Avg E2E: {safe_context["avg_end_to_end_days"]} days
Exception Rate: {safe_context["exception_rate"]}%
"""
        try:
            result = self.llm.chat_json(COMPARISON_SYSTEM, user_prompt)
            return self._normalize_comparison(result, agent_name, process_context)
        except Exception as e:
            return self._fallback_comparison(agent_name, process_context, str(e))

    def _normalize_deep_dive(
        self, result: Dict[str, Any], agent_name: str, process_context: dict
    ) -> Dict[str, Any]:
        workflow = result.get("workflow_prompts", {})
        steps = workflow.get("steps", []) if isinstance(workflow, dict) else []
        if not isinstance(steps, list):
            steps = []

        decision_logic = result.get("decision_logic", [])
        if not isinstance(decision_logic, list):
            decision_logic = []

        guardrails = result.get("guardrails", [])
        if not isinstance(guardrails, list):
            guardrails = []

        if not steps and not decision_logic and not guardrails:
            return self._fallback_deep_dive(agent_name, process_context, "LLM payload missing required sections")

        steps = self._normalize_prompt_blocks(steps, block_type="workflow", process_context=process_context)
        decision_logic = self._normalize_prompt_blocks(
            decision_logic,
            block_type="decision_logic",
            process_context=process_context,
        )
        guardrails = self._normalize_prompt_blocks(guardrails, block_type="guardrail", process_context=process_context)

        critical = result.get("critical_scenario")
        if not isinstance(critical, dict):
            critical = self._critical_scenario(process_context)

        return {
            "agent_name": result.get("agent_name", agent_name),
            "data_source": result.get("data_source", "Celonis Process Mining"),
            "pi_superiority_summary": result.get(
                "pi_superiority_summary",
                "PI enables turnaround-aware prompts, variant-aware confidence, and conformance-aware guardrails that BI-only prompts miss.",
            ),
            "workflow_prompts": {
                "system_prompt": workflow.get("system_prompt", f"{agent_name} execution guide"),
                "steps": steps,
            },
            "decision_logic": decision_logic,
            "guardrails": guardrails,
            "critical_scenario": critical,
        }

    def _fallback_deep_dive(self, agent_name: str, process_context: dict, reason: str) -> Dict[str, Any]:
        durations = process_context.get("activity_durations", {})
        steps: List[Dict[str, Any]] = []
        for idx, (flow, days) in enumerate(list(durations.items())[:5], start=1):
            steps.append(
                {
                    "step": idx,
                    "prompt": f"Execute and monitor transition '{flow}'.",
                    "instruction": f"Execute and monitor transition '{flow}'.",
                    "pi_evidence_used": f"Average transition time is {days} days.",
                    "timing_decision": f"Act before due-date buffer drops below {max(float(days or 0), 1)} days.",
                    "action_impact": "Reduces queue aging and improves on-time completion probability.",
                    "why_bi_only_misses_this": "BI can show open amount but not transition-level lead time.",
                    "source": f"Celonis: throughput mapping for {flow}",
                }
            )

        if not steps:
            steps.append(
                {
                    "step": 1,
                    "prompt": "Validate invoice and route to standard flow.",
                    "instruction": "Validate invoice and route to standard flow.",
                    "pi_evidence_used": "No transition-duration data available for this model snapshot.",
                    "timing_decision": "Use avg end-to-end baseline for conservative routing.",
                    "action_impact": "Maintains safe execution until richer timing data is available.",
                    "why_bi_only_misses_this": "PI still contributes process sequence and conformance context.",
                    "source": "Celonis: process context",
                }
            )

        decision_logic = process_context.get("decision_rules", [])
        if not decision_logic:
            decision_logic = [
                {
                    "prompt": "Use exception signals and path behavior to route case handling.",
                    "condition": "Exception indicators detected in event path",
                    "action": "Route case to exception handling path",
                    "pi_evidence_used": "Exception indicators from event path variants.",
                    "timing_decision": "Escalate when historical resolution time threatens due date.",
                    "action_impact": "Prevents late handling by early triage on high-risk paths.",
                    "why_bi_only_misses_this": "BI-only logic does not account for path-specific resolution lead times.",
                    "confidence": 0.75,
                    "source": "Celonis: fallback decision logic",
                }
            ]

        guardrails = []
        for violation in process_context.get("conformance_violations", []):
            guardrails.append(
                {
                    "type": "compliance",
                    "prompt": "Enforce conformance sequence before posting.",
                    "rule": violation.get("rule", "Enforce conformance sequence"),
                    "pi_evidence_used": "Observed conformance violations in process traces.",
                    "timing_decision": "Block now to avoid longer downstream rework loops.",
                    "action_impact": "Prevents costly reprocessing and audit risk.",
                    "why_bi_only_misses_this": "Only PI surfaces sequence-level deviations, not just status flags.",
                    "source": "Celonis: conformance violations",
                    "enforcement": "BLOCK",
                }
            )

        if not guardrails:
            guardrails.append(
                {
                    "type": "risk",
                    "prompt": "Escalate rare or low-confidence variants for manual review.",
                    "rule": "Escalate low-frequency variants for manual review",
                    "pi_evidence_used": "Variant frequency distribution from process mining.",
                    "timing_decision": "Escalate early to preserve due-date buffer.",
                    "action_impact": "Avoids automation errors on weakly observed paths.",
                    "why_bi_only_misses_this": "BI cannot infer low-frequency variant risk from normalized tables.",
                    "source": "Celonis: variant frequency analysis",
                    "enforcement": "ESCALATE",
                }
            )

        return {
            "agent_name": agent_name,
            "data_source": "Celonis Process Mining",
            "pi_superiority_summary": "Fallback still uses PI timing and conformance evidence rather than static BI rules.",
            "workflow_prompts": {
                "system_prompt": (
                    f"Deterministic fallback prompt generated from Celonis context because LLM output failed ({reason})."
                ),
                "steps": steps,
            },
            "decision_logic": decision_logic,
            "guardrails": guardrails,
            "critical_scenario": self._critical_scenario(process_context),
        }

    def _normalize_comparison(
        self, result: Dict[str, Any], agent_name: str, process_context: dict
    ) -> Dict[str, Any]:
        without_pm = result.get("without_process_mining", {})
        with_pm = result.get("with_process_mining", {})
        differences = result.get("key_differences", [])

        if not isinstance(without_pm, dict) or not isinstance(with_pm, dict) or not isinstance(differences, list):
            return self._fallback_comparison(agent_name, process_context, "LLM payload schema mismatch")

        return {
            "without_process_mining": {
                "prompt": without_pm.get("prompt", ""),
                "limitations": without_pm.get("limitations", []),
            },
            "with_process_mining": {
                "prompt": with_pm.get("prompt", ""),
                "advantages": with_pm.get("advantages", []),
                "celonis_unique_insights": with_pm.get("celonis_unique_insights", []),
            },
            "key_differences": differences,
        }

    @staticmethod
    def _critical_scenario(process_context: dict) -> Dict[str, Any]:
        avg_e2e = float(process_context.get("avg_end_to_end_days", 0) or 0)
        transitions = process_context.get("activity_durations", {}) or {}
        candidate_days = []
        for transition, days in transitions.items():
            transition_lower = str(transition).lower()
            if "invoice" in transition_lower or "payment" in transition_lower:
                candidate_days.append(float(days or 0))
        historical = round(sum(candidate_days) / len(candidate_days), 2) if candidate_days else max(avg_e2e, 0.0)
        return {
            "scenario": "Due in 7 days, but PI historical path duration is 3+ days.",
            "days_until_due": 7,
            "historical_processing_days": historical,
            "recommended_action": "Act now and escalate early if buffer falls below historical processing days.",
            "why_pi_is_better": "PI provides observed turnaround for this path; BI-only views cannot infer path-level lead time.",
        }

    def _normalize_prompt_blocks(
        self,
        blocks: List[Dict[str, Any]],
        block_type: str,
        process_context: dict,
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        baseline_timing = max(float(process_context.get("avg_end_to_end_days", 0) or 0), 1.0)
        for idx, item in enumerate(blocks, start=1):
            if not isinstance(item, dict):
                continue
            prompt_text = item.get("prompt") or item.get("instruction") or item.get("rule") or item.get("action") or ""
            normalized.append(
                {
                    **item,
                    "step": item.get("step", idx if block_type == "workflow" else item.get("step")),
                    "prompt": prompt_text,
                    "pi_evidence_used": item.get(
                        "pi_evidence_used",
                        item.get("source") or item.get("process_context") or "Celonis process context",
                    ),
                    "timing_decision": item.get(
                        "timing_decision",
                        f"Execute before historical buffer of ~{baseline_timing} days is consumed.",
                    ),
                    "action_impact": item.get(
                        "action_impact",
                        "Improves action timing and reduces late-case risk.",
                    ),
                    "why_bi_only_misses_this": item.get(
                        "why_bi_only_misses_this",
                        "BI-only prompts lack variant-level recurrence and transition-level turnaround evidence.",
                    ),
                }
            )
        return normalized

    def _fallback_comparison(self, agent_name: str, process_context: dict, reason: str) -> Dict[str, Any]:
        bottleneck = process_context.get("bottleneck", {})
        exception_rate = process_context.get("exception_rate", 0)
        avg_e2e = process_context.get("avg_end_to_end_days", 0)

        return {
            "without_process_mining": {
                "prompt": (
                    f"{agent_name}: validate invoice fields and route exceptions using static business rules."
                ),
                "limitations": [
                    "No discovered variant frequency context",
                    "No measured transition-level turnaround times",
                    "No mined conformance-violation evidence",
                ],
            },
            "with_process_mining": {
                "prompt": (
                    f"{agent_name}: prioritize actions using Celonis throughput, bottleneck, and exception signals."
                ),
                "advantages": [
                    f"Uses measured average end-to-end time ({avg_e2e} days)",
                    f"Uses observed exception rate ({exception_rate}%)",
                    f"Targets bottleneck transition ({bottleneck.get('activity', 'N/A')})",
                ],
                "celonis_unique_insights": [
                    "Variant frequency-driven confidence and escalation thresholds",
                    "Resource-role mappings tied to actual execution traces",
                    "Conformance-aware guardrails for sequence violations",
                ],
            },
            "key_differences": [
                "Process mining uses observed behavior; BI-only prompt uses static assumptions.",
                "Process mining includes transition-level timing and bottleneck context.",
                f"Fallback comparison generated from deterministic Celonis data ({reason}).",
            ],
        }

    def generate_agent_evidence_context(
            self,
            case_data: Dict[str, Any],
            process_context: Dict[str, Any],
            exception_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Build PI-specific evidence context to inject into prompt.
        Shows why BI alone would miss process insights.
        """
        case_id = case_data.get("case_id")
        activity_trace = case_data.get("activity_trace_text", "")
        duration_days = case_data.get("duration_days", 0.0)

        # Get avg turnaround from process context
        avg_tat = float(process_context.get("avg_end_to_end_days", 0.0) or 0.0)

        # Check if case matches exception patterns
        matching_exceptions = [
            e for e in exception_records
            if str(e.get("case_id")) == str(case_id)
        ]

        # Identify deviations
        deviations = []
        if duration_days > avg_tat * 1.5:
            deviations.append(f"50% slower than golden path ({duration_days:.1f}d vs {avg_tat:.1f}d baseline)")
        if matching_exceptions:
            deviations.append(f"{len(matching_exceptions)} exception flags detected")
        if "moved out" in activity_trace.lower():
            deviations.append("Moved out to manual resolution (indicates processing complication)")

        return {
            "case_id": case_id,
            "activity_trace_highlight": activity_trace,
            "expected_turnaround_days": avg_tat,
            "actual_turnaround_days": duration_days,
            "deviations_from_golden_path": deviations,
            "exception_flags_present": [e.get("exception_type") for e in matching_exceptions],
            "pi_insight": (
                f"BI tools see: invoice #{case_id}, activity sequence, duration. "
                f"Process Intelligence sees: {len(deviations)} deviations from optimal path, "
                f"suggesting resource/process bottleneck, not data quality."
            )
        }
