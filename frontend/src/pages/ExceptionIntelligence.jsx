import React, { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams, Link } from "react-router-dom";
import {
  Alert, Box, Button, Card, CardContent, Chip, CircularProgress,
  Collapse, Grid, Stack, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Typography,
  FormControl, InputLabel, Select, MenuItem, Snackbar,
} from "@mui/material";
import LoadingSpinner from "../components/LoadingSpinner";
import {
  analyzeExceptionRecord, fetchExceptionCategories,
  fetchAllExceptionRecords, sendExceptionToTeams, waitForCacheReady,
} from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const pickData = (r) => { if (!r) return null; if (r.data !== undefined) return r.data; return r; };
const money = (v) => {
  const n = Number(v || 0);
  if (!n) return "N/A";
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K $`;
  return `${n.toFixed(0)} $`;
};
const pct = (v) => `${Number(v || 0).toFixed(1)}%`;
const confidencePct = (v) => {
  const n = Number(v ?? 0);
  if (!Number.isFinite(n)) return "0%";
  return `${Math.round(n <= 1 ? n * 100 : n)}%`;
};
const confidencePercentNumber = (v) => {
  const n = Number(v ?? 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n <= 1 ? n * 100 : n)));
};
const normalizeConfidenceText = (text) => {
  if (!text) return "";
  const convert = (raw) => confidencePct(raw);
  return String(text)
    .replace(/confidence\s+(-?\d+(?:\.\d+)?)/gi, (_m, num) => `confidence ${convert(num)}`)
    .replace(/(-?\d+(?:\.\d+)?)(?=\s*threshold)/gi, (_m, num) => convert(num));
};
const vendorDisplay = (rec) => rec?.vendor_name || rec?.vendor_id || rec?.recurring_vendor_hint || "—";

const RISK_STYLES = {
  CRITICAL: { bg: "#FAEAEA", border: "#E0A0A0", color: "#B03030", dot: "#C94040" },
  HIGH:     { bg: "#FEF3DC", border: "#F0C870", color: "#A05A10", dot: "#C47020" },
  MEDIUM:   { bg: "#EBF2FC", border: "#90B8E8", color: "#1E4E8C", dot: "#2E6EBC" },
  LOW:      { bg: "#E0F0E8", border: "#80C0A0", color: "#1D5C3A", dot: "#2A7A50" },
};

const GUARDRAIL_STATUS_STYLE = {
  pass: { dot: "#3B6D11", bg: "#EAF3DE", title: "#27500A", detail: "#3B6D11", label: "passed" },
  warn: { dot: "#854F0B", bg: "#FAEEDA", title: "#633806", detail: "#854F0B", label: "warning" },
  fail: { dot: "#A32D2D", bg: "#FCEBEB", title: "#791F1F", detail: "#A32D2D", label: "failed" },
};

const toGuardrailSummary = (checks = []) => {
  const passed = checks.filter((c) => c.status === "pass").length;
  const warnings = checks.filter((c) => c.status === "warn").length;
  const failed = checks.filter((c) => c.status === "fail").length;
  if (checks.length > 0 && passed === checks.length) return "all passed";
  const chunks = [];
  if (passed) chunks.push(`${passed} passed`);
  if (warnings) chunks.push(`${warnings} warning${warnings === 1 ? "" : "s"}`);
  if (failed) chunks.push(`${failed} failed`);
  return chunks.join(" · ");
};
const toGuardrailSummaryStyle = (checks = []) => {
  const failed = checks.some((c) => c.status === "fail");
  const warned = checks.some((c) => c.status === "warn");
  if (failed) return { background: "#FCEBEB", color: "#791F1F", border: "1px solid #F09595" };
  if (warned) return { background: "#FAEEDA", color: "#633806", border: "1px solid #FAC775" };
  return { background: "#EAF3DE", color: "#27500A", border: "1px solid #97C459" };
};

function RiskBadge({ risk }) {
  const key = String(risk || "").toUpperCase();
  const s = RISK_STYLES[key] || RISK_STYLES.LOW;
  return (
    <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: s.bg, color: s.color, border: `1px solid ${s.border}`, px: 1, py: 0.25, borderRadius: "99px" }}>
      <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: s.dot, flexShrink: 0 }} />
      <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, fontFamily: G, letterSpacing: "0.05em" }}>{key}</Typography>
    </Box>
  );
}

function SectionLabel({ children, sx = {} }) {
  return (
    <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.08em", color: "#A09890", fontFamily: G, mb: 0.8, ...sx }}>
      {children}
    </Typography>
  );
}

function LabelValue({ label, value }) {
  return (
    <Box sx={{ py: 0.65, borderBottom: "1px solid #F0EDE6" }}>
      <Typography sx={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.07em", color: "#A09890", fontFamily: G, mb: 0.3 }}>{label}</Typography>
      <Typography sx={{ fontSize: "0.78rem", color: "#4C4840", fontFamily: G, lineHeight: 1.55 }}>{value || "N/A"}</Typography>
    </Box>
  );
}

function PanelCard({ title, accentColor, bg, border, children }) {
  return (
    <Card sx={{ height: "100%", background: `${bg} !important`, border: `1px solid ${border} !important`, borderTop: `2px solid ${accentColor} !important` }}>
      <CardContent sx={{ pb: "16px !important" }}>
        <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: accentColor, mb: 1.2 }}>{title}</Typography>
        {children}
      </CardContent>
    </Card>
  );
}

export default function ExceptionIntelligence() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const [loading, setLoading] = useState(true);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [loadingTeams, setLoadingTeams] = useState(false);
  const [error, setError] = useState("");
  const [records, setRecords] = useState([]);
  const [selectedRecordId, setSelectedRecordId] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const [agentGuardrailSteps, setAgentGuardrailSteps] = useState([]);
  const [agentContextOpen, setAgentContextOpen] = useState(false);
  const [toast, setToast] = useState({ open: false, message: "", severity: "success" });
  const analysisRequestRef = useRef(0);

  // ── Load all records across all categories (flat) ──
  useEffect(() => {
    let active = true;
    const abortController = new AbortController();
    (async () => {
      setLoading(true);
      setError("");
      try {
        let categoriesRes = await fetchExceptionCategories();
        let categories = (Array.isArray(pickData(categoriesRes)) ? pickData(categoriesRes) : [])
          .filter((row) => Number(row.case_count || 0) > 0)
          .sort((a, b) => Number(b.case_count || 0) - Number(a.case_count || 0));
        if (categories.length === 0) {
          await waitForCacheReady({ signal: abortController.signal });
          categoriesRes = await fetchExceptionCategories();
          categories = (Array.isArray(pickData(categoriesRes)) ? pickData(categoriesRes) : [])
            .filter((row) => Number(row.case_count || 0) > 0)
            .sort((a, b) => Number(b.case_count || 0) - Number(a.case_count || 0));
        }
        const categoryMap = new Map(categories.map((category) => [category.category_id, category]));
        const recordsRes = await fetchAllExceptionRecords();
        const rows = Array.isArray(pickData(recordsRes)) ? pickData(recordsRes) : [];
        const flattened = rows
          .map((row) => {
            const category = categoryMap.get(row.category_id) || null;
            return {
              ...row,
              category_label: row.category_label || category?.category_label || row.exception_type,
            };
          })
          .sort((a, b) => Number(b.invoice_amount || b.value_at_risk || 0) - Number(a.invoice_amount || a.value_at_risk || 0));
        if (!active) return;
        setRecords(flattened);

        // Pre-select from URL param if present, else first record
        const paramId = searchParams.get("exception_id");
        const preselect = paramId
          ? (flattened.find((r) => r.exception_id === paramId) || flattened[0])
          : flattened[0];
        if (preselect) setSelectedRecordId(preselect.exception_id);
      } catch (e) {
        if (!active) return;
        setError(e?.response?.data?.detail || e.message || "Failed to load exceptions.");
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => { active = false; abortController.abort(); };
  }, []);

  const selectedRecord = useMemo(
    () => records.find((r) => r.exception_id === selectedRecordId) || null,
    [records, selectedRecordId]
  );

  const selectedIndex = useMemo(
    () => records.findIndex((r) => r.exception_id === selectedRecordId),
    [records, selectedRecordId]
  );
  // TODO: This is the live backend hook consumption point for guardrail strip rendering in Case Resolution.
  const guardrailChecks = useMemo(() => {
    if (!Array.isArray(analysis?.guardrail_results)) return [];
    return analysis.guardrail_results.map((item, idx) => ({
      ruleId: item?.rule_id || item?.ruleId || `RULE_${idx + 1}`,
      label: item?.label || item?.title || "Guardrail",
      status: String(item?.status || "").toLowerCase(),
      detail: normalizeConfidenceText(item?.detail || item?.reason || ""),
      enforcement: item?.enforcement || "code",
      agentName: item?.agent_name || item?.agentName || "ExceptionAgent",
    }));
  }, [analysis]);
  const guardrailSummary = useMemo(() => toGuardrailSummary(guardrailChecks), [guardrailChecks]);
  const guardrailSummaryStyle = useMemo(() => toGuardrailSummaryStyle(guardrailChecks), [guardrailChecks]);
  const guardrailTrigger = useMemo(() => {
    const triggered = guardrailChecks.find((c) => c.status === "fail" || c.status === "warn") || null;
    if (!triggered) return null;
    return `Guardrail trigger: ${triggered.ruleId} fired on ${triggered.agentName} — ${triggered.detail}`;
  }, [guardrailChecks]);
  const routingFinalStatus = Boolean(analysis?.send_to_human_review) ? "ESCALATED_TO_HUMAN" : analysis?.automation_decision || "MONITOR";
  const routingUrgency = analysis?.turnaround_risk?.risk_level || "MEDIUM";
  const routingEta = analysis?.turnaround_risk?.estimated_processing_days != null ? `${Number(analysis.turnaround_risk.estimated_processing_days).toFixed(2)}d` : "N/A";
  const routingDecision = analysis?.classifier_agent?.decision || analysis?.automation_decision || "MONITOR";

  // ── Run analysis whenever selected record changes ──
  useEffect(() => {
    if (!selectedRecord) return;
    let active = true;
    (async () => {
      const requestId = ++analysisRequestRef.current;
      setAnalysisLoading(true);
      setAnalysis(null);
      setAgentGuardrailSteps([]);
      setAgentContextOpen(false);
      try {
        const payload = {
          exception_type: selectedRecord.category_label || selectedRecord.exception_type,
          exception_id: selectedRecord.exception_id,
          invoice_id: selectedRecord.invoice_id || selectedRecord.document_number || selectedRecord.case_id || "",
          vendor_id: selectedRecord.vendor_id || "",
          vendor_name: vendorDisplay(selectedRecord),
          invoice_amount: selectedRecord.invoice_amount || selectedRecord.value_at_risk || 0,
          currency: selectedRecord.currency || "USD",
          days_until_due: selectedRecord.days_until_due || 0,
          extra_context: selectedRecord,
        };
        const data = pickData(await analyzeExceptionRecord(payload));
        if (!active || analysisRequestRef.current !== requestId) return;
        setAnalysis(data);
        setAgentGuardrailSteps(Array.isArray(data?.agent_guardrail_steps) ? data.agent_guardrail_steps : []);
      } catch (e) {
        if (!active || analysisRequestRef.current !== requestId) return;
        setError(e?.response?.data?.detail || e.message || "Failed to analyze exception.");
      } finally {
        if (active && analysisRequestRef.current === requestId) setAnalysisLoading(false);
      }
    })();
    return () => { active = false; };
  }, [selectedRecord]);

  const sendToTeams = async () => {
    if (!analysis) return;
    setLoadingTeams(true);
    try {
      const d = pickData(await sendExceptionToTeams(analysis));
      setToast({ open: true, message: d?.success ? "Sent to Microsoft Teams." : "Failed to send to Teams.", severity: d?.success ? "success" : "error" });
    } catch (e) {
      setToast({ open: true, message: e?.response?.data?.detail || e.message || "Failed.", severity: "error" });
    } finally {
      setLoadingTeams(false);
    }
  };

  if (loading) return <LoadingSpinner message="Building exception intelligence from Celonis records..." />;

  return (
    <div className="page-container">
      {/* ── Page Header ── */}
      <Box sx={{ pt: 4, pb: 2, borderBottom: "1px solid #ECEAE4", mb: 2.5 }}>
        {/* Breadcrumb */}
        <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1 }}>
          <Box
            onClick={() => navigate("/exceptions-workbench")}
            sx={{ display: "inline-flex", alignItems: "center", gap: 0.4, cursor: "pointer", color: "#9C9690", fontSize: "0.78rem", fontFamily: G, "&:hover": { color: "#B5742A" }, transition: "color 0.15s" }}
          >
            ← Exception Triage
          </Box>
          <Typography sx={{ color: "#C8C0B4", fontSize: "0.78rem", fontFamily: G }}>/</Typography>
          <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G }}>Case Resolution</Typography>
        </Box>

        <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
          <Box>
            <Typography sx={{ fontFamily: S, fontSize: "2.1rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.3 }}>
              Case Resolution
            </Typography>
            <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>
              Deep analysis per exception — process path, root cause, and action decisions.
            </Typography>
          </Box>

          {/* Case Jump / Switch */}
          {records.length > 0 && (
            <FormControl size="small" sx={{ minWidth: 240 }}>
              <InputLabel sx={{ fontFamily: G, fontSize: "0.8rem" }}>Switch Case</InputLabel>
              <Select
                value={selectedRecordId}
                label="Switch Case"
                onChange={(e) => setSelectedRecordId(e.target.value)}
                sx={{ fontFamily: G, fontSize: "0.82rem" }}
              >
                {records.map((rec) => (
                  <MenuItem key={rec.exception_id} value={rec.exception_id} sx={{ fontFamily: G, fontSize: "0.82rem" }}>
                    {rec.invoice_id || rec.exception_id} — {vendorDisplay(rec)}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
        </Box>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2, fontFamily: G }}>{error}</Alert>}

      {/* ── Sticky Case Header Bar ── */}
      {selectedRecord && (
        <Card sx={{ mb: 2.5, border: "1px solid #ECEAE4 !important", position: "sticky", top: "56px", zIndex: 100, background: "#FFFFFF !important" }}>
          <CardContent sx={{ pb: "14px !important" }}>
            <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 1.5 }}>
              {/* Left: identity */}
              <Box>
                <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 0.2 }}>
                  {selectedRecord.category_label || selectedRecord.exception_type || "Exception"}
                </Typography>
                <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>
                  Invoice <strong style={{ color: "#5C5650" }}>{selectedRecord.invoice_id || selectedRecord.document_number || selectedRecord.case_id || "N/A"}</strong>
                  {" · "}Vendor <strong style={{ color: "#5C5650" }}>{vendorDisplay(selectedRecord)}</strong>
                </Typography>
              </Box>

              {/* Centre: chips */}
              <Stack direction="row" spacing={0.8} flexWrap="wrap" alignItems="center">
                <Chip size="small" label={money(selectedRecord.invoice_amount || selectedRecord.value_at_risk)} sx={{ background: "#FEF3DC", color: "#A05A10", border: "1px solid #F0C870", fontFamily: G, fontSize: "0.7rem", height: 22 }} />
                <Chip size="small" label={`DPO ${Number(selectedRecord.dpo || selectedRecord.actual_dpo || selectedRecord.avg_resolution_time_days || 0).toFixed(1)}d`} sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8", fontFamily: G, fontSize: "0.7rem", height: 22 }} />
                <Chip size="small" label={`Freq ${pct(selectedRecord.frequency_percentage)}`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E4E0D8", fontFamily: G, fontSize: "0.7rem", height: 22 }} />
                <RiskBadge risk={selectedRecord.risk_level || "MEDIUM"} />
              </Stack>

              {/* Right: prev / next */}
              <Stack direction="row" spacing={0.8}>
                <Button
                  size="small"
                  variant="outlined"
                  disabled={selectedIndex <= 0}
                  onClick={() => setSelectedRecordId(records[selectedIndex - 1].exception_id)}
                  sx={{ fontFamily: G, fontSize: "0.75rem", minWidth: 36, px: 1 }}
                >
                  ‹ Prev
                </Button>
                <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G, alignSelf: "center" }}>
                  {selectedIndex + 1} / {records.length}
                </Typography>
                <Button
                  size="small"
                  variant="outlined"
                  disabled={selectedIndex >= records.length - 1}
                  onClick={() => setSelectedRecordId(records[selectedIndex + 1].exception_id)}
                  sx={{ fontFamily: G, fontSize: "0.75rem", minWidth: 36, px: 1 }}
                >
                  Next ›
                </Button>
              </Stack>
            </Box>

            {/* AI summary inline */}
            {analysis?.summary && (
              <Typography sx={{ fontSize: "0.8rem", color: "#6C6660", fontFamily: G, lineHeight: 1.65, mt: 1.2, pt: 1.2, borderTop: "1px solid #F0EDE6" }}>
                {analysis.summary}
              </Typography>
            )}
          </CardContent>
        </Card>
      )}

      {/* ── Analysis Body ── */}
      {!selectedRecord ? (
        <Card sx={{ border: "1px solid #ECEAE4 !important" }}>
          <CardContent>
            <Box sx={{ py: 6, textAlign: "center" }}>
              <Typography sx={{ fontSize: "0.85rem", color: "#B0A898", fontFamily: G }}>
                Select an exception to inspect its analysis.
              </Typography>
            </Box>
          </CardContent>
        </Card>
      ) : analysisLoading ? (
        <LoadingSpinner message="Analyzing selected exception with process context..." />
      ) : (
        <Stack spacing={2}>

          {/* ══ THREE-PANEL ROW ══ */}
          <Grid container spacing={2}>

            {/* Panel 1 — Process Intelligence (what happened) */}
            <Grid item xs={12} md={4}>
              <PanelCard title="Process Intelligence" accentColor="#1A6B5E" bg="#F7FBF9" border="#C0E8DC">

                {/* Happy Path */}
                <SectionLabel>Happy Path</SectionLabel>
                <LabelValue label="Path" value={analysis?.happy_path?.path} />
                <LabelValue label="Avg Duration" value={analysis?.happy_path?.avg_duration_days != null ? `${Number(analysis.happy_path.avg_duration_days).toFixed(1)} days` : null} />
                {analysis?.happy_path?.why_it_matters && (
                  <LabelValue label="Why it matters" value={analysis.happy_path.why_it_matters} />
                )}

                {/* Exception Path */}
                <Box sx={{ mt: 1.5 }}>
                  <SectionLabel>Exception Path</SectionLabel>
                  <LabelValue label="Observed Path" value={analysis?.exception_path?.path || selectedRecord?.summary} />
                  <LabelValue label="Exception Stage" value={analysis?.exception_path?.exception_stage} />
                  <LabelValue label="Extra Delay" value={analysis?.exception_path?.extra_days != null ? `${Number(analysis.exception_path.extra_days).toFixed(1)} days` : null} />
                </Box>

                {/* Consolidated Celonis Signals */}
                {analysis?.exception_context_from_celonis && (
                  <Box sx={{ mt: 1.5, pt: 1.2, borderTop: "1px solid #C0E8DC" }}>
                    <SectionLabel>Celonis Signals</SectionLabel>
                    {analysis.exception_context_from_celonis.category_summary && (
                      <Typography sx={{ fontSize: "0.75rem", color: "#2A6050", fontFamily: G, lineHeight: 1.55, mb: 1 }}>
                        {analysis.exception_context_from_celonis.category_summary}
                      </Typography>
                    )}
                    {[
                      ["Process Steps", (analysis.exception_context_from_celonis.process_step_signals || []).join(" | ")],
                      ["Variants",      (analysis.exception_context_from_celonis.variant_signals || []).join(" | ")],
                      ["Cycle Time",    (analysis.exception_context_from_celonis.cycle_time_signals || []).join(" | ")],
                    ].map(([k, v]) => v ? (
                      <Box key={k} sx={{ mb: 0.5 }}>
                        <Typography component="span" sx={{ fontSize: "0.7rem", fontWeight: 700, color: "#1A6B5E", fontFamily: G }}>{k}: </Typography>
                        <Typography component="span" sx={{ fontSize: "0.7rem", color: "#3A7060", fontFamily: G }}>{v}</Typography>
                      </Box>
                    ) : null)}
                  </Box>
                )}
              </PanelCard>
            </Grid>

            {/* Panel 2 — Why it matters (root cause, breach risk, financial impact) */}
            <Grid item xs={12} md={4}>
              <PanelCard title="Risk & Root Cause" accentColor="#B03030" bg="#FDF7F7" border="#F0C8C8">

                {/* Root cause */}
                <SectionLabel>Root Cause</SectionLabel>
                <Box sx={{ background: "#FAEAEA", border: "1px solid #E0A0A0", borderLeft: "3px solid #B03030", borderRadius: "0 8px 8px 0", p: 1.2, mb: 1.2 }}>
                  <Typography sx={{ fontSize: "0.78rem", fontWeight: 600, color: "#4C4840", fontFamily: G, mb: 0.4 }}>
                    {analysis?.root_cause_analysis?.most_likely_cause || "N/A"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.74rem", color: "#7C7670", fontFamily: G, lineHeight: 1.6 }}>
                    {analysis?.root_cause_analysis?.why || analysis?.root_cause_analysis?.celonis_evidence || ""}
                  </Typography>
                </Box>

                {/* Breach probability */}
                {analysis?.breach_probability != null && (
                  <Box sx={{ mb: 1.2 }}>
                    <SectionLabel>Breach Probability</SectionLabel>
                    <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 0.5 }}>
                      <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G }}>SLA breach risk</Typography>
                      <Box sx={{ background: Number(analysis.breach_probability) >= 70 ? "#FAEAEA" : "#FEF3DC", color: Number(analysis.breach_probability) >= 70 ? "#B03030" : "#A05A10", border: `1px solid ${Number(analysis.breach_probability) >= 70 ? "#E0A0A0" : "#F0C870"}`, px: 1, py: 0.2, borderRadius: "6px" }}>
                        <Typography sx={{ fontSize: "0.8rem", fontWeight: 700, fontFamily: G }}>{Number(analysis.breach_probability).toFixed(0)}%</Typography>
                      </Box>
                    </Box>
                    <Box sx={{ height: 4, background: "#F0EDE6", borderRadius: "99px", overflow: "hidden" }}>
                      <Box sx={{ height: "100%", width: `${Math.min(100, Number(analysis.breach_probability))}%`, background: Number(analysis.breach_probability) >= 70 ? "#C94040" : "#C47020", borderRadius: "99px", transition: "width 0.5s ease" }} />
                    </Box>
                  </Box>
                )}

                {/* Stage timing */}
                <SectionLabel>Stage Timing</SectionLabel>
                <LabelValue
                  label="Current Stage"
                  value={analysis?.root_cause_analysis?.process_stage || analysis?.exception_context_from_celonis?.category_summary || "—"}
                />
                <LabelValue
                  label="Time in Stage"
                  value={analysis?.turnaround_risk?.estimated_processing_days != null ? `${Number(analysis.turnaround_risk.estimated_processing_days).toFixed(1)} days` : null}
                />
                <LabelValue
                  label="Historical Avg"
                  value={analysis?.turnaround_risk?.historical_avg_days != null ? `${Number(analysis.turnaround_risk.historical_avg_days).toFixed(1)} days` : null}
                />
                <LabelValue
                  label="75th Percentile"
                  value={analysis?.turnaround_risk?.percentile75_days != null ? `${Number(analysis.turnaround_risk.percentile75_days).toFixed(1)} days` : null}
                />
                {analysis?.turnaround_risk?.estimated_processing_days != null &&
                  analysis?.turnaround_risk?.percentile75_days != null &&
                  analysis.turnaround_risk.estimated_processing_days > analysis.turnaround_risk.percentile75_days && (
                  <Box sx={{ mt: 0.8, display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "99px", px: 1, py: 0.3 }}>
                    <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: "#C47020" }} />
                    <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>Above 75th percentile</Typography>
                  </Box>
                )}

                {/* Financial impact */}
                {analysis?.financial_impact && (
                  <Box sx={{ mt: 1.2 }}>
                    <SectionLabel>Financial Impact</SectionLabel>
                    <LabelValue label="Value at Risk" value={money(analysis.financial_impact.value_at_risk)} />
                    <LabelValue label="Potential Savings" value={money(analysis.financial_impact.potential_savings)} />
                  </Box>
                )}
              </PanelCard>
            </Grid>

            {/* Panel 3 — What to do (next best action + classifier) */}
            <Grid item xs={12} md={4}>
              <PanelCard title="Recommended Action" accentColor="#A05A10" bg="#FFFDF7" border="#EDD090">

                {/* Next Best Action */}
                <LabelValue label="Recommended Action" value={analysis?.next_best_action?.action} />
                <LabelValue label="Why" value={analysis?.next_best_action?.why} />
                <LabelValue label="ETA" value={analysis?.turnaround_risk?.estimated_processing_days != null ? `${analysis.turnaround_risk.estimated_processing_days} days` : null} />

                {/* Classifier */}
                <Box sx={{ mt: 1.2, pt: 1.2, borderTop: "1px solid #EDD090" }}>
                  <SectionLabel>Classifier Decision</SectionLabel>
                  <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 0.8 }}>
                    <Box sx={{ px: 1, py: 0.3, borderRadius: "999px", border: "1px solid #E4E0D8", background: "#F5F3EF" }}>
                      <Typography sx={{ fontSize: "0.72rem", color: "#5C5650", fontFamily: G, fontWeight: 600 }}>
                        {analysis?.classifier_agent?.decision || analysis?.automation_decision || "MONITOR"}
                      </Typography>
                    </Box>
                    <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G, fontWeight: 600 }}>
                      {confidencePct(analysis?.classifier_agent?.confidence)}
                    </Typography>
                  </Box>
                  <Box sx={{ height: 5, background: "#F0EDE6", borderRadius: "99px", overflow: "hidden" }}>
                    <Box
                      sx={{
                        height: "100%",
                        width: `${confidencePercentNumber(analysis?.classifier_agent?.confidence)}%`,
                        background: "#B5742A",
                        borderRadius: "99px",
                        transition: "width 0.5s ease",
                      }}
                    />
                  </Box>
                </Box>
              </PanelCard>
            </Grid>
          </Grid>

          {guardrailChecks.length > 0 && (
            <Card sx={{ border: "1px solid #ECEAE4 !important" }}>
              <CardContent sx={{ pb: "14px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1 }}>
                  <Typography sx={{ fontSize: "11px", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.05em", color: "#A09890", fontFamily: G }}>
                    Guardrail checks — before action
                  </Typography>
                  <Box sx={{ px: "8px", py: "2px", borderRadius: "20px", ...guardrailSummaryStyle }}>
                    <Typography sx={{ fontSize: "11px", fontFamily: G }}>{guardrailSummary}</Typography>
                  </Box>
                </Box>
                <Stack spacing={0.7}>
                  {guardrailChecks.map((check, idx) => {
                    const style = GUARDRAIL_STATUS_STYLE[check.status] || GUARDRAIL_STATUS_STYLE.warn;
                    return (
                      <Box
                        key={`${check.ruleId || idx}`}
                        sx={{ p: "8px 10px", borderRadius: "8px", background: style.bg, display: "flex", gap: 0.9, alignItems: "flex-start" }}
                      >
                        <Box sx={{ width: 7, height: 7, borderRadius: "50%", background: style.dot, mt: "4px", flexShrink: 0 }} />
                        <Box>
                          <Typography sx={{ fontSize: "12px", color: style.title, fontFamily: G, fontWeight: 500 }}>
                            {check.label} — {style.label}
                          </Typography>
                          <Typography sx={{ fontSize: "12px", color: style.detail, fontFamily: G, lineHeight: 1.45, mt: "1px" }}>
                            {check.detail}
                          </Typography>
                          <Typography sx={{ fontSize: "11px", color: "#9C9690", fontFamily: G, mt: "2px" }}>
                            Rule: {check.ruleId} · enforcement: {check.enforcement || "code"}
                          </Typography>
                        </Box>
                      </Box>
                    );
                  })}
                </Stack>
              </CardContent>
            </Card>
          )}

          <Card sx={{ border: "1px solid #ECEAE4 !important" }}>
            <CardContent sx={{ pb: "14px !important" }}>
              <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#A05A10", mb: 0.9 }}>
                Routing Outcome
              </Typography>
              <Typography sx={{ fontSize: "0.78rem", color: "#6C6660", fontFamily: G, mb: 0.5 }}>
                Final status: {routingFinalStatus} | Urgency: {routingUrgency} | ETA {routingEta}
              </Typography>
              <Typography sx={{ fontSize: "0.78rem", color: "#7A5010", fontFamily: G, lineHeight: 1.55 }}>
                Automation posture is {routingDecision} based on process risk, root cause, and due-date timing context.
              </Typography>
              <Typography sx={{ fontSize: "0.78rem", color: "#7A5010", fontFamily: G, lineHeight: 1.55 }}>
                Auto route / human decision: {routingDecision} · Teams handoff ready: {Boolean(analysis?.send_to_human_review) ? "Yes" : "No"}
              </Typography>
              {guardrailTrigger && (
                <Typography sx={{ fontSize: "12px", color: "#A05A10", fontFamily: G, mt: 0.5 }}>
                  {guardrailTrigger}
                </Typography>
              )}
            </CardContent>
          </Card>

          {/* ══ Collapsible Agent Routing Context ══ */}
          {analysis?.prompt_for_next_agents && (
            <Card sx={{ border: "1px solid #ECEAE4 !important" }}>
              <CardContent sx={{ pb: "14px !important" }}>
                <Box
                  sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}
                  onClick={() => setAgentContextOpen((o) => !o)}
                >
                  <Typography sx={{ fontFamily: S, fontSize: "1rem", color: "#9C9690" }}>
                    Agent Routing Context
                  </Typography>
                  <Typography sx={{ fontSize: "0.78rem", color: "#B5742A", fontFamily: G }}>
                    {agentContextOpen ? "Hide ▲" : "Show ▼"}
                  </Typography>
                </Box>
                <Collapse in={agentContextOpen}>
                  <Box sx={{ mt: 1.5, pt: 1.5, borderTop: "1px solid #ECEAE4" }}>
                    <Grid container spacing={1.5}>
                      {[
                        ["Target Agents", (analysis.prompt_for_next_agents.target_agents || []).join(", ")],
                        ["Handoff Intent", analysis.prompt_for_next_agents.handoff_intent],
                        ["PI Rationale", analysis.prompt_for_next_agents.pi_rationale],
                      ].map(([k, v]) => (
                        <Grid item xs={12} md={4} key={k}>
                          <Box sx={{ p: 1.2, background: "#FEF8EE", border: "1px solid #EDD090", borderRadius: "8px", height: "100%" }}>
                            <SectionLabel sx={{ mb: 0.4 }}>{k}</SectionLabel>
                            <Typography sx={{ fontSize: "0.75rem", color: "#7A5010", fontFamily: G, lineHeight: 1.55 }}>{v || "N/A"}</Typography>
                          </Box>
                        </Grid>
                      ))}
                    </Grid>
                  </Box>
                </Collapse>
              </CardContent>
            </Card>
          )}

          {/* ══ Sticky Action Commit Zone ══ */}
          <Box sx={{
            position: "sticky", bottom: 0, left: 0, right: 0,
            background: "rgba(247,245,240,0.95)", backdropFilter: "blur(10px)",
            borderTop: "1px solid #ECEAE4", py: 1.5, px: 0,
            mx: -3, zIndex: 200,
          }}>
            <Box sx={{ maxWidth: "1380px", mx: "auto", px: 3, display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 1.5 }}>
              <Box>
                <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G }}>
                  Actions for <strong style={{ color: "#5C5650" }}>{selectedRecord?.invoice_id || selectedRecord?.exception_id}</strong>
                </Typography>
                {analysis?.classifier_agent?.decision && (
                  <Typography sx={{ fontSize: "0.72rem", color: "#A09890", fontFamily: G }}>
                    Classifier: {analysis.classifier_agent.decision}
                  </Typography>
                )}
              </Box>
              <Stack direction="row" spacing={1} flexWrap="wrap">
                <Button
                  variant="outlined"
                  sx={{ fontFamily: G, fontSize: "0.8rem", borderRadius: "8px", textTransform: "none", px: 2 }}
                  onClick={() => navigate("/exceptions-workbench")}
                >
                  ← Back to Triage
                </Button>
                <Button
                  variant="outlined"
                  sx={{ fontFamily: G, fontSize: "0.8rem", borderRadius: "8px", textTransform: "none", px: 2 }}
                  disabled={!analysis}
                >
                  Approve Auto-Resolution
                </Button>
                <Button
                  variant="outlined"
                  sx={{ fontFamily: G, fontSize: "0.8rem", borderRadius: "8px", textTransform: "none", px: 2 }}
                  disabled={!analysis}
                >
                  Escalate to Specialist
                </Button>
                <Button
                  variant="contained"
                  onClick={sendToTeams}
                  disabled={loadingTeams || !analysis}
                  sx={{
                    background: "#B5742A !important", fontFamily: G, fontSize: "0.8rem",
                    fontWeight: 600, borderRadius: "8px", textTransform: "none", px: 2.5,
                    "&:hover": { background: "#9A6020 !important" },
                  }}
                >
                  {loadingTeams ? "Sending…" : "Send to Teams"}
                </Button>
              </Stack>
            </Box>
          </Box>

        </Stack>
      )}

      <Snackbar
        open={toast.open}
        autoHideDuration={3500}
        onClose={() => setToast((t) => ({ ...t, open: false }))}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      >
        <Alert severity={toast.severity} onClose={() => setToast((t) => ({ ...t, open: false }))} sx={{ fontFamily: G }}>
          {toast.message}
        </Alert>
      </Snackbar>
    </div>
  );
}
