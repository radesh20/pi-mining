import React, { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Box, Button, Card, CardContent, Chip, Grid, Stack, TextField, Typography } from "@mui/material";
import InteractionFlow from "../components/InteractionFlow";
import LoadingSpinner from "../components/LoadingSpinner";
import { analyzeExceptionRecord, executeInvoiceFlow, fetchAllExceptionRecords, fetchExceptionCategories, waitForCacheReady } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";
const numberFields = new Set(["invoice_amount", "days_in_exception", "actual_dpo", "potential_dpo", "days_until_due"]);
const FORM_FIELDS = [["invoice_id", "Invoice ID"], ["vendor_id", "Vendor ID"], ["vendor_name", "Vendor Name"], ["invoice_amount", "Invoice Amount", "number"], ["currency", "Currency"], ["invoice_payment_terms", "Invoice Payment Terms"], ["po_payment_terms", "PO Payment Terms"], ["vendor_master_terms", "Vendor Master Terms"], ["payment_due_date", "Payment Due Date"], ["days_until_due", "Days Until Due", "number"], ["days_in_exception", "Days in Exception", "number"], ["actual_dpo", "Actual DPO", "number"], ["potential_dpo", "Potential DPO", "number"], ["company_code", "Company Code"], ["scenario", "Scenario Notes"]];

const money = (v) => {
  const n = Number(v || 0);
  if (!n) return "N/A";
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K $`;
  return `${n.toFixed(0)} $`;
};

const riskStyle = (value) => {
  const v = String(value || "").toUpperCase();
  if (v === "CRITICAL") return { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0" };
  if (v === "HIGH") return { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" };
  if (v === "MEDIUM") return { bg: "#EBF2FC", color: "#1E4E8C", border: "#90B8E8" };
  return { bg: "#DCF0EB", color: "#1A6B5E", border: "#8FCFC5" };
};

const toInvoicePayload = (record) => {
  if (!record) {
    return {
      invoice_id: "",
      vendor_id: "",
      vendor_name: "",
      invoice_amount: 0,
      currency: "USD",
      invoice_payment_terms: "",
      po_payment_terms: "",
      vendor_master_terms: "",
      payment_due_date: "",
      days_until_due: 0,
      days_in_exception: 0,
      actual_dpo: 0,
      potential_dpo: 0,
      company_code: "",
      scenario: "",
    };
  }
  return {
    invoice_id: record.invoice_id || record.document_number || record.case_id || "",
    vendor_id: record.vendor_id || "",
    vendor_name: record.vendor_name || record.vendor_id || "",
    invoice_amount: Number(record.invoice_amount || record.value_at_risk || 0),
    currency: record.currency || "USD",
    invoice_payment_terms: record.invoice_payment_terms || record.payment_terms || "",
    po_payment_terms: record.po_payment_terms || record.payment_terms || "",
    vendor_master_terms: record.vendor_master_terms || "",
    payment_due_date: record.payment_due_date || "",
    days_until_due: Number(record.days_until_due || 0),
    days_in_exception: Number(record.days_in_exception || record.avg_resolution_time_days || 0),
    actual_dpo: Number(record.actual_dpo || record.dpo || 0),
    potential_dpo: Number(record.potential_dpo || record.actual_dpo || record.dpo || 0),
    company_code: record.company_code || "",
    scenario: record.exception_type || record.summary || "Celonis exception scenario",
  };
};

const buildAnalysisPayload = (record) => ({
  exception_type: record.exception_type || "",
  exception_id: record.exception_id,
  invoice_id: record.invoice_id || record.document_number || record.case_id || "",
  vendor_id: record.vendor_id || "",
  vendor_name: record.vendor_name || record.vendor_id || "",
  invoice_amount: record.invoice_amount || record.value_at_risk || 0,
  currency: record.currency || "USD",
  days_until_due: record.days_until_due || 0,
  extra_context: record,
});

function MetricCard({ label, value, caption, color }) {
  return (
    <Card>
      <CardContent>
        <Typography sx={{ fontSize: "0.69rem", textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>{label}</Typography>
        <Typography sx={{ fontFamily: S, fontSize: "2rem", color, mb: 0.3 }}>{value}</Typography>
        <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>{caption}</Typography>
      </CardContent>
    </Card>
  );
}

export default function CrossAgentInteraction() {
  const [categories, setCategories] = useState([]);
  const [records, setRecords] = useState([]);
  const [selectedRecordId, setSelectedRecordId] = useState("");
  const [invoice, setInvoice] = useState(toInvoicePayload(null));
  const [analysis, setAnalysis] = useState(null);
  const [loadingPage, setLoadingPage] = useState(true);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);
  const [loadingFlow, setLoadingFlow] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const analysisRequestRef = useRef(0);

  const selectedRecord = useMemo(
    () => records.find((record) => record.exception_id === selectedRecordId) || null,
    [records, selectedRecordId],
  );
  const executionTrace = result?.execution_trace || result?.data?.execution_trace || null;

  useEffect(() => {
    let active = true;
    const load = async (retryIfCacheCold = true) => {
      try {
        const [categoriesRes, recordsRes] = await Promise.all([
          fetchExceptionCategories(),
          fetchAllExceptionRecords(),
        ]);
        const categoryRows = (categoriesRes.data || categoriesRes || []).filter((row) => Number(row.case_count || 0) > 0);
        const recordRows = (recordsRes.data || recordsRes || []).filter((row) => row.exception_id);
        if (retryIfCacheCold && categoryRows.length === 0 && recordRows.length === 0) {
          await waitForCacheReady();
          return await load(false);
        }
        if (!active) return;
        setCategories(categoryRows);
        setRecords(recordRows);
        const first = recordRows[0] || null;
        if (first) {
          setSelectedRecordId(first.exception_id);
          setInvoice(toInvoicePayload(first));
        }
      } catch (e) {
        if (!active) return;
        setError(e?.response?.data?.detail || e.message || "Failed to load cross-agent data");
      } finally {
        if (active) setLoadingPage(false);
      }
    };
    load();
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!selectedRecord) return;
    setInvoice(toInvoicePayload(selectedRecord));
    setResult(null);
    const requestId = ++analysisRequestRef.current;
    setLoadingAnalysis(true);
    analyzeExceptionRecord(buildAnalysisPayload(selectedRecord))
      .then((res) => {
        if (analysisRequestRef.current !== requestId) return;
        setAnalysis(res.data || res);
      })
      .catch((e) => {
        if (analysisRequestRef.current !== requestId) return;
        setError(e?.response?.data?.detail || e.message || "Failed to analyze selected exception");
      })
      .finally(() => {
        if (analysisRequestRef.current === requestId) setLoadingAnalysis(false);
      });
  }, [selectedRecord]);

  const handleField = (field, value) => {
    setInvoice((prev) => ({ ...prev, [field]: numberFields.has(field) ? Number(value || 0) : value }));
  };

  const runOrchestration = async () => {
    setLoadingFlow(true);
    setError("");
    setResult(null);
    try {
      setResult(await executeInvoiceFlow(invoice));
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || "Execution failed");
    } finally {
      setLoadingFlow(false);
    }
  };

  const totalValueAtRisk = records.reduce((sum, record) => sum + Number(record.invoice_amount || record.value_at_risk || 0), 0);
  const autoCandidates = records.filter((record) => {
    const risk = String(record.risk_level || "").toUpperCase();
    return !["CRITICAL", "HIGH"].includes(risk);
  }).length;
  const exceptionAgentPrompt = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Exception Agent"))?.full_output?.prompt_for_next_agents;
  const automationDecision = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Automation Policy Agent"))?.full_output;
  const humanStep = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Human-in-the-Loop Agent"))?.full_output;

  if (loadingPage) return <LoadingSpinner message="Loading Celonis-derived cross-agent scenarios..." />;

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Cross-Agent Interaction
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Follow how the Invoice Processing Agent and Exception Agent exchange Celonis-derived prompts, timing context, routing decisions, next best actions, and human-review escalation.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}><MetricCard label="Exception Scenarios" value={records.length} caption="Real exception records available for orchestration" color="#1E4E8C" /></Grid>
        <Grid item xs={12} md={3}><MetricCard label="Categories" value={categories.length} caption="Distinct Celonis exception buckets" color="#B5742A" /></Grid>
        <Grid item xs={12} md={3}><MetricCard label="Value At Risk" value={money(totalValueAtRisk)} caption="Invoice exposure across the loaded queue" color="#B03030" /></Grid>
        <Grid item xs={12} md={3}><MetricCard label="Auto Candidates" value={autoCandidates} caption="Lower-risk records likely fit for auto-route" color="#1A6B5E" /></Grid>
      </Grid>

      <Grid container spacing={2.5}>
        <Grid item xs={12} md={5}>
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Celonis Queue</Typography>
              <Stack spacing={0.9}>
                {records.slice(0, 10).map((record) => {
                  const active = selectedRecordId === record.exception_id;
                  const style = riskStyle(record.risk_level || "LOW");
                  return (
                    <Box
                      key={record.exception_id}
                      onClick={() => setSelectedRecordId(record.exception_id)}
                      sx={{ p: 1.25, borderRadius: "10px", border: active ? "2px solid #B5742A" : "1px solid #E8E3DA", background: active ? "#F5ECD9" : "#FDFCFA", cursor: "pointer" }}
                    >
                      <Typography sx={{ fontSize: "0.82rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>{record.exception_type || "Exception"}</Typography>
                      <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
                        {record.invoice_id || record.document_number || record.case_id} · {record.vendor_name || record.vendor_id || "Unknown vendor"}
                      </Typography>
                      <Stack direction="row" spacing={0.8} alignItems="center" flexWrap="wrap" gap={0.6}>
                        <Chip size="small" label={money(record.invoice_amount || record.value_at_risk || 0)} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }} />
                        <Chip size="small" label={`DPO ${Number(record.actual_dpo || record.dpo || 0).toFixed(1)}`} sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" }} />
                        <Chip size="small" label={String(record.risk_level || "LOW").toUpperCase()} sx={{ background: style.bg, color: style.color, border: `1px solid ${style.border}` }} />
                      </Stack>
                    </Box>
                  );
                })}
              </Stack>
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Execution Payload</Typography>
              <Grid container spacing={1.2}>
                {FORM_FIELDS.map(([field, label, type]) => (
                  <Grid key={field} item xs={12} sm={field === "scenario" ? 12 : 6}>
                    <TextField fullWidth size="small" label={label} value={invoice[field] ?? ""} type={type || "text"} onChange={(e) => handleField(field, e.target.value)} />
                  </Grid>
                ))}
              </Grid>
              <Button variant="contained" fullWidth onClick={runOrchestration} disabled={loadingFlow || !selectedRecord} sx={{ mt: 2 }}>
                {loadingFlow ? "Executing..." : "Run Invoice + Exception Flow"}
              </Button>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={7}>
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Prompt Interaction Summary</Typography>
              {loadingAnalysis ? (
                <LoadingSpinner message="Analyzing selected exception with process context..." />
              ) : analysis ? (
                <>
                  <Box sx={{ p: 1.2, background: "#EBF2FC", border: "1px solid #90B8E8", borderRadius: "10px", mb: 1.2 }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1E4E8C", fontFamily: G, mb: 0.5 }}>Invoice Processing Agent Prompt Outcome</Typography>
                    <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>{analysis.summary}</Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G }}>Turnaround risk: {analysis.turnaround_risk?.risk_level || "MEDIUM"} · ETA {Number(analysis.turnaround_risk?.estimated_processing_days || 0).toFixed(2)} days</Typography>
                  </Box>
                  <Box sx={{ p: 1.2, background: "#F7FBF9", border: "1px solid #CFE5DA", borderRadius: "10px", mb: 1.2 }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", fontFamily: G, mb: 0.5 }}>Exception Agent Prompt Outcome</Typography>
                    <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>{analysis.next_best_action?.action || "No next best action available."}</Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#1A6B5E", fontFamily: G, mb: 0.3 }}>{analysis.next_best_action?.why}</Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G }}>Auto route: {analysis.classifier_agent?.recommended_mode || "human_review"} · Human review: {analysis.send_to_human_review ? "Yes" : "No"}</Typography>
                  </Box>
                  <Box sx={{ p: 1.2, background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "10px" }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.5 }}>Prompt Handoff</Typography>
                    <Typography sx={{ fontSize: "0.8rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>{analysis.prompt_for_next_agents?.execution_prompt || "Execution prompt not available."}</Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G }}>Targets: {(analysis.prompt_for_next_agents?.target_agents || []).join(", ") || "N/A"}</Typography>
                  </Box>
                </>
              ) : (
                <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>Select a queue record to inspect prompt interaction.</Typography>
              )}
            </CardContent>
          </Card>

          {loadingFlow && <LoadingSpinner message="Running orchestration with Celonis handoff context..." />}
          {executionTrace && <InteractionFlow executionTrace={executionTrace} />}

          {executionTrace && (
            <>
              <Card sx={{ mt: 2 }}>
                <CardContent>
                  <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Routing Outcome</Typography>
                  <Stack direction="row" spacing={1} flexWrap="wrap" gap={0.8} sx={{ mb: 1 }}>
                    <Chip size="small" label={`Final status: ${executionTrace.final_status || "UNKNOWN"}`} sx={{ background: "#F5ECD9", color: "#B5742A", border: "1px solid #DEC48A" }} />
                    <Chip size="small" label={`Urgency: ${executionTrace.turnaround_assessment?.urgency || "MEDIUM"}`} sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" }} />
                    <Chip size="small" label={`ETA ${Number(executionTrace.turnaround_assessment?.estimated_processing_days || 0).toFixed(2)}d`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }} />
                  </Stack>
                  <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
                    {automationDecision?.reasoning || executionTrace.turnaround_assessment?.recommendation || "Routing decision derived from Celonis timing and exception context."}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#1A6B5E", fontFamily: G }}>
                    Auto route / human decision: {automationDecision?.automation_decision || analysis?.classifier_agent?.recommended_mode || "MONITOR"} · Teams handoff ready: {humanStep ? "Yes" : "Pending"}
                  </Typography>
                </CardContent>
              </Card>

              <Card sx={{ mt: 2 }}>
                <CardContent>
                  <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Exception Resolution + Human Loop</Typography>
                  <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
                    Next best action: {exceptionAgentPrompt?.execution_prompt || analysis?.next_best_action?.action || "N/A"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G, mb: 0.3 }}>
                    Handoff intent: {exceptionAgentPrompt?.handoff_intent || "Exception resolution handoff"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#B03030", fontFamily: G, mb: 0.3 }}>
                    Human review package: {humanStep?.case_summary || humanStep?.reason_for_review || "Will be prepared when escalation is required."}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G }}>
                    Teams-ready evidence: {humanStep?.celonis_evidence || analysis?.exception_context_from_celonis?.category_summary || "Celonis evidence attached in the HITL package."}
                  </Typography>
                </CardContent>
              </Card>
            </>
          )}
        </Grid>
      </Grid>
    </div>
  );
}
