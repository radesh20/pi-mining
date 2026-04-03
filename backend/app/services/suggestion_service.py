"""
app/services/suggestion_service.py

Generates follow-up question suggestions based on user messages,
AI responses, and process mining context.
"""

import logging
from typing import Any, Dict, List, Optional

from app.services.azure_openai_service import AzureOpenAIService

logger = logging.getLogger(__name__)


class SuggestionService:
    """
    Generates intelligent follow-up questions for the chatbot.
    Uses LLM to understand context and suggest relevant next questions.
    """

    def __init__(self, llm: AzureOpenAIService):
        self.llm = llm

    def generate_suggestions(
        self,
        user_message: str,
        ai_reply: str,
        context_used: Dict[str, Any],
        case_id: Optional[str] = None,
        vendor_id: Optional[str] = None,
    ) -> List[str]:
        """
        Generate 3-4 follow-up question suggestions based on the conversation.

        Args:
            user_message: The original user question
            ai_reply: The AI's answer
            context_used: The PI context data that was used
            case_id: Optional case scope
            vendor_id: Optional vendor scope

        Returns:
            List of 3-4 suggested follow-up questions
        """
        try:
            # Build context summary for the LLM
            context_summary = self._build_context_summary(context_used, case_id, vendor_id)

            # Craft prompt to generate suggestions
            suggestion_prompt = f"""Based on this conversation, suggest 3-4 relevant follow-up questions 
that would help the user dive deeper into the process intelligence insights.

USER'S ORIGINAL QUESTION:
{user_message}

YOUR RESPONSE:
{ai_reply}

PROCESS CONTEXT AVAILABLE:
{context_summary}

Generate follow-up questions that:
1. Explore related process areas
2. Dig deeper into any mentioned issues
3. Ask about recommended actions or next steps
4. Compare performance across dimensions (time, vendors, agents)

Return ONLY the questions, one per line, without numbering or bullets.
Keep each question concise (under 15 words).
"""

            # Call LLM to generate suggestions
            suggestions_text = self.llm.chat(
                system_prompt="You are an expert at generating follow-up questions for process intelligence analysis.",
                user_prompt=suggestion_prompt,
                temperature=0.7,  # Slightly higher temp for more creative suggestions
                max_tokens=300,
            )

            # Parse the response into individual questions
            suggestions = [q.strip() for q in suggestions_text.split("\n") if q.strip()]

            # Ensure we return 3-4 suggestions
            suggestions = suggestions[:4]
            if len(suggestions) < 3:
                suggestions.extend(self._get_fallback_suggestions(context_used, case_id, vendor_id))
                suggestions = suggestions[:4]

            logger.info(f"Generated {len(suggestions)} follow-up suggestions")
            return suggestions

        except Exception as exc:
            logger.warning(f"Could not generate suggestions via LLM: {str(exc)}")
            return self._get_fallback_suggestions(context_used, case_id, vendor_id)

    @staticmethod
    def _build_context_summary(
        context_used: Dict[str, Any],
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> str:
        """Build a summary of available PI context."""
        summary_parts = []

        # Global stats
        global_ctx = context_used.get("global", {})
        if global_ctx:
            summary_parts.append(
                f"- {global_ctx.get('total_cases', 'N/A')} total cases, "
                f"{global_ctx.get('avg_end_to_end_days', 'N/A')} days avg cycle time, "
                f"{global_ctx.get('exception_rate', 'N/A')}% exception rate"
            )

        # Bottleneck info
        bottleneck = global_ctx.get("bottleneck", {})
        if bottleneck:
            summary_parts.append(
                f"- Bottleneck: {bottleneck.get('activity', 'N/A')} "
                f"({bottleneck.get('duration_days', 'N/A')} days avg)"
            )

        # Agents
        agents_ctx = context_used.get("agents", {})
        if agents_ctx.get("agent_count", 0) > 0:
            summary_parts.append(f"- {agents_ctx.get('agent_count')} active agents available")

        # Scoped context
        if case_id:
            case_ctx = context_used.get("case", {})
            summary_parts.append(
                f"- Case {case_id}: {case_ctx.get('current_stage', 'Unknown')} stage, "
                f"{case_ctx.get('days_in_process', 'N/A')} days in process"
            )

        if vendor_id:
            vendor_ctx = context_used.get("vendor", {})
            summary_parts.append(
                f"- Vendor {vendor_id}: {vendor_ctx.get('total_cases', 'N/A')} cases, "
                f"{vendor_ctx.get('exception_rate', 'N/A')}% exception rate"
            )

        return "\n".join(summary_parts) if summary_parts else "General process intelligence context available"

    @staticmethod
    def _get_fallback_suggestions(
        context_used: Dict[str, Any],
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> List[str]:
        """Fallback suggestions based on context when LLM generation fails."""
        suggestions = []

        # Global bottleneck question
        bottleneck = context_used.get("global", {}).get("bottleneck", {})
        if bottleneck.get("activity"):
            suggestions.append(f"What drives the {bottleneck['activity']} bottleneck?")

        # Agent-related question
        agents = context_used.get("agents", {}).get("recommended_agents", [])
        if agents:
            first_agent = agents[0].get("agent_name", "automation")
            suggestions.append(f"How would {first_agent} improve this process?")

        # Case-specific question
        if case_id:
            suggestions.append(f"What's the expected completion timeline for Case {case_id}?")

        # Vendor-specific question
        if vendor_id:
            suggestions.append(f"How does {vendor_id} compare to similar vendors?")

        # Default questions
        if not suggestions:
            suggestions = [
                "What are the top process improvement opportunities?",
                "Which cases have the highest exception risk?",
                "What process variants are causing delays?",
            ]

        return suggestions[:4]