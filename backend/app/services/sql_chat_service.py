"""
app/services/sql_chat_service.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.azure_openai_service import AzureOpenAIService

SQL_SYSTEM_PROMPT = """You are a SAP AP analytics assistant.
Answer using only the data-search results provided below.

STYLE RULES:
1. Keep the answer very short and crisp.
2. Use at most 3 bullet points.
3. First bullet must directly answer the user question.
4. Include exact values/counts when available.
5. If filtered data is missing, clearly say so in one short line.
6. Do not mention Celonis. Do not output SQL unless the user explicitly asks for SQL.
7. End with a single line exactly starting with "-> Recommended action:".

DATA SEARCH CONTEXT
{search_context}
"""


class SQLChatService:
    _cached_knowledge_db: Optional[Dict[str, Any]] = None
    _cached_table_frames: Optional[Dict[str, pd.DataFrame]] = None

    def __init__(self, llm: AzureOpenAIService):
        self.llm = llm
        self.knowledge_db = self._cached_knowledge_db or self._load_knowledge_db()
        SQLChatService._cached_knowledge_db = self.knowledge_db

        if self._cached_table_frames is None:
            SQLChatService._cached_table_frames = self._load_source_tables()
        self.table_frames = SQLChatService._cached_table_frames or {}

    def chat(
        self,
        message: str,
        conversation_history: List[Dict[str, str]],
        table_name: Optional[str] = None,
        dialect: str = "PostgreSQL",
        case_id: Optional[str] = None,
        vendor_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        search_ctx = self._build_search_context(
            message=message,
            table_name=table_name,
            case_id=case_id,
            vendor_id=vendor_id,
        )

        if search_ctx["total_matches"] == 0 and (case_id or vendor_id):
            filters = " + ".join(
                [
                    x
                    for x in [
                        f"Case {case_id}" if case_id else "",
                        f"Vendor {vendor_id}" if vendor_id else "",
                    ]
                    if x
                ]
            )
            reply = (
                f"- No records found for **{filters}** in the knowledge database.\n"
                "- Try another case/vendor ID or remove one filter."
            )
        else:
            prompt_context = search_ctx["context_text"]
            system_prompt = SQL_SYSTEM_PROMPT.format(search_context=prompt_context)
            user_prompt = self._build_user_prompt(message, conversation_history)
            reply = self.llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.1,
                max_tokens=350,
            )

        recommended_action = self._recommended_action(
            total_matches=search_ctx["total_matches"],
            case_id=case_id,
            vendor_id=vendor_id,
        )
        if "-> Recommended action:" not in reply:
            reply = f"{reply.strip()}\n\n-> Recommended action: {recommended_action}"

        suggested_questions = self._clarifying_questions(
            message=message,
            conversation_history=conversation_history,
            case_id=case_id,
            vendor_id=vendor_id,
        )
        next_steps = [recommended_action]

        data_sources: List[str] = []
        if self.knowledge_db:
            data_sources = [
                "Knowledge DB (uploaded SAP tables)",
                f"Tables: {self.knowledge_db.get('table_count', 0)}",
                f"Relationships: {self.knowledge_db.get('relationship_count', 0)}",
            ]
        else:
            data_sources.append("Knowledge DB not available")

        if case_id:
            data_sources.append(f"Case scope - {case_id}")
        if vendor_id:
            data_sources.append(f"Vendor scope - {vendor_id}")
        data_sources.append(f"Matching rows scanned - {search_ctx['total_matches']}")

        if case_id and vendor_id:
            scope_label = f"Case {case_id} + Vendor {vendor_id}"
        elif case_id:
            scope_label = f"Case {case_id}"
        elif vendor_id:
            scope_label = f"Vendor {vendor_id}"
        else:
            scope_label = "Global DB"

        return {
            "success": True,
            "reply": reply,
            "suggested_questions": suggested_questions,
            "data_sources": data_sources,
            "next_steps": next_steps,
            "context_used": {
                "table_name": table_name or "",
                "dialect": dialect or "PostgreSQL",
                "case_id": case_id or "",
                "vendor_id": vendor_id or "",
                "matched_tables": search_ctx["matched_tables"],
            },
            "scope_label": scope_label,
            "agent_used": "Knowledge DB Query Assistant",
            "error": None,
        }

    @staticmethod
    def _load_knowledge_db() -> Optional[Dict[str, Any]]:
        root = Path(__file__).resolve().parents[2]
        file_path = root / "data" / "excel_knowledge_db.json"
        if not file_path.exists():
            return None
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _load_source_tables(self) -> Dict[str, pd.DataFrame]:
        if not self.knowledge_db:
            return {}
        source_files = self.knowledge_db.get("source_files", {})
        frames: Dict[str, pd.DataFrame] = {}
        for table_name, file_path in source_files.items():
            try:
                df = pd.read_excel(file_path)
                df.columns = [str(c).strip() for c in df.columns]
                frames[table_name] = df
            except Exception:
                continue
        return frames

    @staticmethod
    def _norm(v: Any) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s.upper()

    @staticmethod
    def _norm_name(v: str) -> str:
        return "".join(ch.lower() for ch in str(v) if ch.isalnum())

    def _find_columns(self, df: pd.DataFrame, aliases: List[str]) -> List[str]:
        alias_norm = {self._norm_name(a) for a in aliases}
        cols: List[str] = []
        for c in df.columns:
            cn = self._norm_name(c)
            if cn in alias_norm:
                cols.append(c)
        return cols

    def _filter_with_id(self, df: pd.DataFrame, id_value: str, candidate_columns: List[str]) -> pd.Series:
        target = self._norm(id_value)
        mask = pd.Series([False] * len(df), index=df.index)
        if not target:
            return mask
        for col in candidate_columns:
            ser = df[col].astype(str).str.strip().str.upper()
            exact = ser == target
            contains = ser.str.contains(target, regex=False, na=False)
            mask = mask | exact | contains
        return mask

    def _build_search_context(
        self,
        message: str,
        table_name: Optional[str],
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> Dict[str, Any]:
        if not self.table_frames:
            return {
                "context_text": "No source data loaded.",
                "total_matches": 0,
                "matched_tables": [],
            }

        case_aliases = ["CaseKey", "Belnr", "Invoice Number", "Awkey", "Ebeln", "Banfn"]
        vendor_aliases = ["Lifnr", "Supplier Number", "Vendor", "Vendor ID", "Name1"]

        if table_name and table_name in self.table_frames:
            tables_to_scan = [table_name]
        else:
            tables_to_scan = list(self.table_frames.keys())

        lines: List[str] = []
        matched_tables: List[Dict[str, Any]] = []
        total_matches = 0

        lines.append(f"User question: {message}")
        lines.append(f"Filter Case ID: {case_id or 'none'}")
        lines.append(f"Filter Vendor ID: {vendor_id or 'none'}")
        lines.append("Matched table evidence:")

        for t in tables_to_scan:
            df = self.table_frames[t]
            current = df
            case_cols = self._find_columns(df, case_aliases)
            vendor_cols = self._find_columns(df, vendor_aliases)

            if case_id:
                if not case_cols:
                    continue
                case_mask = self._filter_with_id(current, case_id, case_cols)
                current = current[case_mask]

            if vendor_id:
                if not vendor_cols:
                    continue
                vendor_mask = self._filter_with_id(current, vendor_id, vendor_cols)
                current = current[vendor_mask]

            if current.empty and (case_id or vendor_id):
                continue

            sample_cols = [str(c) for c in current.columns[:8]]
            sample_rows = current.head(2).to_dict(orient="records")
            compact_rows = []
            for row in sample_rows:
                compact_rows.append({k: str(v) for k, v in row.items() if pd.notna(v)})

            row_count = int(len(current))
            total_matches += row_count
            matched_tables.append(
                {
                    "table": t,
                    "rows": row_count,
                    "case_columns": case_cols,
                    "vendor_columns": vendor_cols,
                }
            )
            lines.append(f"- {t}: {row_count} rows matched")
            lines.append(f"  Columns: {', '.join(sample_cols)}")
            if compact_rows:
                lines.append(f"  Sample rows: {compact_rows}")

        if not matched_tables and not case_id and not vendor_id:
            for t in tables_to_scan[:5]:
                df = self.table_frames[t]
                lines.append(f"- {t}: total_rows={len(df)} (no scope filter)")

        return {
            "context_text": "\n".join(lines),
            "total_matches": total_matches,
            "matched_tables": matched_tables,
        }

    @staticmethod
    def _build_user_prompt(message: str, conversation_history: List[Dict[str, str]]) -> str:
        history_lines: List[str] = []
        for msg in conversation_history[-6:]:
            role = msg.get("role", "user").upper()
            content = (msg.get("content", "") or "").strip()
            if content:
                history_lines.append(f"{role}: {content}")
        history_block = "\n".join(history_lines) if history_lines else "No previous context."
        return f"Conversation so far:\n{history_block}\n\nCurrent user request:\n{message.strip()}"

    @staticmethod
    def _clarifying_questions(
        message: str,
        conversation_history: List[Dict[str, str]],
        case_id: Optional[str],
        vendor_id: Optional[str],
    ) -> List[str]:
        text = (message or "").lower()
        asked_text = " ".join(
            [
                str(m.get("content", "")).lower()
                for m in conversation_history[-12:]
                if isinstance(m, dict)
            ]
        )

        if any(k in text for k in ["invoice", "amount", "value", "total"]):
            intent_pool = [
                "Do you want amount totals by document type or by posting date?",
                "Should I compare gross value vs converted value for this scope?",
            ]
        elif any(k in text for k in ["vendor", "supplier", "lifnr"]):
            intent_pool = [
                "Should I include vendor master details (name and country) as well?",
                "Do you want only this vendor's AP records or PO records too?",
            ]
        elif any(k in text for k in ["status", "stuck", "delay", "pending", "aging", "ageing"]):
            intent_pool = [
                "Should I prioritize pending/stuck indicators or full lifecycle summary?",
                "Do you want the latest status only or all historical events?",
            ]
        else:
            intent_pool = [
                "Should I break this down by AP tables vs PO tables?",
                "Do you want a quick summary or record-level detail?",
            ]

        scope_pool: List[str] = []
        if not case_id:
            scope_pool.append("Can you share the Case ID to narrow this answer?")
        if not vendor_id:
            scope_pool.append("Can you share the Vendor ID for a vendor-specific answer?")
        if case_id and vendor_id:
            scope_pool.extend(
                [
                    "Should I focus on consistency checks between this case and vendor records?",
                    "Do you want case timeline first or vendor-level summary first?",
                ]
            )

        candidates = scope_pool + intent_pool
        deduped: List[str] = []
        for q in candidates:
            q_norm = q.lower()
            if q_norm in asked_text:
                continue
            if q not in deduped:
                deduped.append(q)
        return deduped[:2]

    @staticmethod
    def _recommended_action(total_matches: int, case_id: Optional[str], vendor_id: Optional[str]) -> str:
        if total_matches == 0:
            return "Validate Case ID/Vendor ID format and retry with one scope first."
        if case_id and vendor_id:
            return "Use this scoped result to investigate mismatches between case and vendor records."
        if case_id:
            return "Drill into linked vendor and PO records for this case to confirm root cause."
        if vendor_id:
            return "Review all cases for this vendor and compare outliers by amount and status."
        return "Provide Case ID or Vendor ID to get a precise answer."
