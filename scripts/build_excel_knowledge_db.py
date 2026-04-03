#!/usr/bin/env python3
"""
Build a JSON knowledge database from the user-provided Excel files.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


BASE_FILES: Dict[str, str] = {
    "TCURF": "/Users/radeshchakravarthy/Desktop/TCURF.xlsx",
    "TCURR": "/Users/radeshchakravarthy/Desktop/TCURR.xlsx",
    "TCURX": "/Users/radeshchakravarthy/Desktop/TCURX.xlsx",
    "VIM_HEADER": "/Users/radeshchakravarthy/Desktop/VIM header.xlsx",
    "AP_BKPF": "/Users/radeshchakravarthy/Desktop/Ap BKPF.xlsx",
    "AP_BSEG": "/Users/radeshchakravarthy/Desktop/Ap BSEG.xlsx",
    "AP_RSEG": "/Users/radeshchakravarthy/Desktop/AP_RSEG.xlsx",
    "AR_AGEING": "/Users/radeshchakravarthy/Desktop/AR_Ageing_Sheet1 (1).xlsx",
    "EBAN": "/Users/radeshchakravarthy/Desktop/EBAN.xlsx",
    "EKBE": "/Users/radeshchakravarthy/Desktop/EKBE (2).xlsx",
    "EKKO": "/Users/radeshchakravarthy/Desktop/EKKO.xlsx",
    "EKPO": "/Users/radeshchakravarthy/Desktop/EKPO.xlsx",
    "LFA1": "/Users/radeshchakravarthy/Desktop/LFA1.xlsx",
}

RELATIONSHIP_SEEDS = [
    ("EKKO", "Ebeln", "EKPO", "Ebeln", "1-to-many", "PO header to PO items"),
    ("EKPO", "Ebeln", "EKBE", "Ebeln", "1-to-many", "PO item to PO history"),
    ("EKPO", "Ebelp", "EKBE", "Ebelp", "1-to-many", "PO item line to PO history line"),
    ("EBAN", "Ebeln", "EKPO", "Ebeln", "1-to-many", "PR to PO conversion"),
    ("EBAN", "Banfn", "EKPO", "Banfn", "1-to-many", "Purchase requisition number"),
    ("EBAN", "Bnfpo", "EKPO", "Bnfpo", "1-to-many", "Purchase requisition item"),
    ("EKKO", "Lifnr", "LFA1", "Lifnr", "many-to-1", "PO vendor master"),
    ("AP_BKPF", "Belnr", "AP_BSEG", "Invoice Number", "1-to-many", "FI document header to lines"),
    ("AP_BKPF", "Bukrs", "AP_BSEG", "Bukrs", "1-to-many", "Company code alignment"),
    ("AP_BKPF", "Gjahr", "AP_BSEG", "Gjahr", "1-to-many", "Fiscal year alignment"),
    ("AP_RSEG", "Ebeln Ekpo", "EKPO", "Ebeln", "many-to-1", "Invoice receipt to PO item"),
    ("AP_RSEG", "Ebelp Ekpo", "EKPO", "Ebelp", "many-to-1", "Invoice receipt line to PO line"),
    ("AP_RSEG", "Zterm Ekko", "EKKO", "Zterm", "many-to-1", "Payment term mapping"),
    ("AP_RSEG", "Waers Ekko", "EKKO", "Waers", "many-to-1", "Currency mapping"),
    ("TCURR", "Fcurr", "TCURX", "Currkey", "many-to-1", "Exchange rate source currency"),
    ("TCURR", "Tcurr", "TCURX", "Currkey", "many-to-1", "Exchange rate target currency"),
    ("TCURF", "Fcurr", "TCURX", "Currkey", "many-to-1", "Conversion factor source currency"),
    ("TCURF", "Tcurr", "TCURX", "Currkey", "many-to-1", "Conversion factor target currency"),
    ("AP_BKPF", "Waers", "TCURX", "Currkey", "many-to-1", "FI currency metadata"),
    ("EKKO", "Waers", "TCURX", "Currkey", "many-to-1", "PO currency metadata"),
]


def _norm_value(v) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.endswith(".0"):
        s = s[:-2]
    return s.upper()


def _norm_col(name: str) -> str:
    return "".join(ch.lower() for ch in str(name) if ch.isalnum())


def _load_tables() -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for table, file_path in BASE_FILES.items():
        df = pd.read_excel(file_path)
        df.columns = [str(c).strip() for c in df.columns]
        tables[table] = df
    return tables


def _column_profile(df: pd.DataFrame, col: str) -> Dict:
    ser = df[col]
    non_null = int(ser.notna().sum())
    uniq = int(ser.nunique(dropna=True))
    sample_values: List[str] = []
    for v in ser.head(12).tolist():
        nv = _norm_value(v)
        if nv is not None and nv not in sample_values:
            sample_values.append(nv)
        if len(sample_values) >= 6:
            break
    return {
        "name": col,
        "normalized_name": _norm_col(col),
        "dtype": str(ser.dtype),
        "non_null_count": non_null,
        "unique_count": uniq,
        "null_count": int(len(df) - non_null),
        "key_candidate": bool(len(df) > 0 and non_null == len(df) and uniq == len(df)),
        "sample_values": sample_values,
    }


def _table_summary(df: pd.DataFrame, rows: int = 4) -> List[Dict]:
    if df.empty:
        return []
    sample = df.head(rows).copy()
    output: List[Dict] = []
    for _, row in sample.iterrows():
        item = {}
        for c in sample.columns:
            v = row[c]
            if pd.isna(v):
                item[c] = None
            elif isinstance(v, (pd.Timestamp, datetime)):
                item[c] = str(v)
            else:
                item[c] = str(v)
        output.append(item)
    return output


def _build_relationships(tables: Dict[str, pd.DataFrame]) -> List[Dict]:
    rels: List[Dict] = []
    for lt, lc, rt, rc, cardinality, note in RELATIONSHIP_SEEDS:
        ldf = tables.get(lt)
        rdf = tables.get(rt)
        if ldf is None or rdf is None or lc not in ldf.columns or rc not in rdf.columns:
            rels.append(
                {
                    "left_table": lt,
                    "left_column": lc,
                    "right_table": rt,
                    "right_column": rc,
                    "cardinality": cardinality,
                    "note": note,
                    "status": "column_missing",
                    "overlap_ratio": None,
                }
            )
            continue
        lset = {_norm_value(v) for v in ldf[lc].dropna().tolist()}
        rset = {_norm_value(v) for v in rdf[rc].dropna().tolist()}
        lset.discard(None)
        rset.discard(None)
        inter = lset.intersection(rset)
        overlap = round((len(inter) / len(lset)) if lset else 0.0, 4)
        confidence = "high" if overlap >= 0.8 else "medium" if overlap >= 0.5 else "low"
        rels.append(
            {
                "left_table": lt,
                "left_column": lc,
                "right_table": rt,
                "right_column": rc,
                "cardinality": cardinality,
                "note": note,
                "left_distinct": len(lset),
                "right_distinct": len(rset),
                "matching_distinct_values": len(inter),
                "overlap_ratio": overlap,
                "confidence": confidence,
                "status": "verified",
            }
        )
    return rels


def build_knowledge_db() -> Dict:
    tables = _load_tables()
    table_objects = []
    for table_name, df in tables.items():
        table_objects.append(
            {
                "table_name": table_name,
                "source_file": BASE_FILES[table_name],
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "columns": [_column_profile(df, c) for c in df.columns],
                "sample_rows": _table_summary(df),
            }
        )

    relationships = _build_relationships(tables)
    return {
        "dataset_name": "SAP_AP_Excel_Knowledge_DB",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_type": "excel",
        "source_files": BASE_FILES,
        "table_count": len(table_objects),
        "relationship_count": len(relationships),
        "tables": table_objects,
        "relationships": relationships,
        "query_guidelines": [
            "Prefer EKKO -> EKPO -> EKBE for purchasing document lineage.",
            "Join EKKO.Lifnr to LFA1.Lifnr for vendor master attributes.",
            "Join AP_BKPF to AP_BSEG on Belnr/Invoice Number plus Bukrs and Gjahr.",
            "Use TCURR/TCURF/TCURX for currency conversion metadata.",
            "Use AP_RSEG to bridge invoice receipt details with EKPO/EKKO purchasing context.",
        ],
    }


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = repo_root / "backend" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "excel_knowledge_db.json"
    payload = build_knowledge_db()
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote: {out_file}")
    print(f"Tables: {payload['table_count']}, Relationships: {payload['relationship_count']}")


if __name__ == "__main__":
    main()
