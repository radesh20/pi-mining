import React, { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Grid,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import {
  analyzeExceptionRecord,
  fetchExceptionCategories,
  fetchExceptionRecords,
  fetchNextBestAction,
  sendExceptionToTeams,
} from "../api/client";

const money = (value) => {
  const n = Number(value || 0);
  if (!n) return "N/A";
  if (n >= 1000000) return `${(n / 1000000).toFixed(2)}M $`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}K $`;
  return `${n.toFixed(0)} $`;
};

const pickData = (res) => {
  if (!res) return null;
  if (res.data !== undefined) return res.data;
  return res;
};

const riskColor = (risk) => {
  const value = String(risk || "").toUpperCase();
  if (value === "CRITICAL") return "error";
  if (value === "HIGH") return "warning";
  if (value === "MEDIUM") return "warning";
  return "success";
};

const quickBadge = (record) => {
  const text = `${record.summary || ""} ${record.exception_type || ""}`.toLowerCase();
  if (text.includes("mismatch")) return "Mismatch";
  if (text.includes("late") || text.includes("due")) return "Late";
  if (text.includes("early") || text.includes("optimization")) return "Optimization";
  if (text.includes("short") || text.includes("0-day")) return "Short Terms";
  return "Exception";
};

export default function ExceptionsWorkbench() {
  const [categories, setCategories] = useState([]);
  const [selectedCategoryId, setSelectedCategoryId] = useState("");
  const [records, setRecords] = useState([]);
  const [selectedRecord, setSelectedRecord] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [nextAction, setNextAction] = useState(null);

  const [loadingCategories, setLoadingCategories] = useState(true);
  const [loadingRecords, setLoadingRecords] = useState(false);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);
  const [loadingTeams, setLoadingTeams] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState({ open: false, message: "", severity: "success" });

  useEffect(() => {
    let active = true;
    fetchExceptionCategories()
      .then((res) => {
        const data = pickData(res);
        const rows = Array.isArray(data) ? data : [];
        if (!active) return;
        setCategories(rows);
        if (rows.length > 0) {
          const first = rows[0];
          setSelectedCategoryId(first.category_id || "");
        }
      })
      .catch((e) => {
        if (!active) return;
        setError(e?.response?.data?.detail || e.message || "Failed to load exception categories");
      })
      .finally(() => {
        if (active) setLoadingCategories(false);
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedCategoryId) return;
    setLoadingRecords(true);
    setRecords([]);
    setSelectedRecord(null);
    setAnalysis(null);
    setNextAction(null);
    fetchExceptionRecords(selectedCategoryId)
      .then((res) => {
        const data = pickData(res);
        setRecords(Array.isArray(data) ? data : []);
      })
      .catch((e) => setError(e?.response?.data?.detail || e.message || "Failed to load records"))
      .finally(() => setLoadingRecords(false));
  }, [selectedCategoryId]);

  const selectedCategoryObject = useMemo(
    () => categories.find((c) => c.category_id === selectedCategoryId),
    [categories, selectedCategoryId]
  );

  const runAnalysis = async (record) => {
    setLoadingAnalysis(true);
    setSelectedRecord(record);
    setAnalysis(null);
    setNextAction(null);
    try {
      const payload = {
        exception_type: selectedCategoryObject?.category_label || selectedCategoryId,
        exception_id: record.exception_id,
        invoice_id: record.invoice_id || "",
        vendor_id: record.vendor_id || record.recurring_vendor_hint || "",
        vendor_name: record.vendor_name || "",
        invoice_amount: record.invoice_amount || 0,
        currency: record.currency || "USD",
        extra_context: record,
      };
      const analysisRes = await analyzeExceptionRecord(payload);
      const analysisData = pickData(analysisRes);
      setAnalysis(analysisData);

      const nbaRes = await fetchNextBestAction(analysisData);
      setNextAction(pickData(nbaRes));
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || "Failed to analyze exception");
    } finally {
      setLoadingAnalysis(false);
    }
  };

  const sendToTeams = async () => {
    if (!analysis) return;
    setLoadingTeams(true);
    try {
      const res = await sendExceptionToTeams(analysis);
      const data = pickData(res);
      const ok = Boolean(data?.success);
      setToast({
        open: true,
        message: ok ? "Sent to Microsoft Teams successfully." : "Failed to send to Teams.",
        severity: ok ? "success" : "error",
      });
    } catch (e) {
      setToast({
        open: true,
        message: e?.response?.data?.detail || e.message || "Failed to send to Teams.",
        severity: "error",
      });
    } finally {
      setLoadingTeams(false);
    }
  };

  return (
    <div className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, color: "text.primary", mb: 0.5 }}>
        Exceptions Workbench
      </Typography>
      <Typography variant="body1" sx={{ color: "text.secondary", mb: 2 }}>
        Analyze exceptions, predict next best action, and escalate to Teams when human review is required.
      </Typography>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={2}>
        <Grid item xs={12} md={5}>
          <Card sx={{ mb: 2, border: "1px solid #e5e7eb" }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 1.5, color: "text.primary" }}>
                Exception Categories
              </Typography>
              {loadingCategories ? (
                <CircularProgress size={22} />
              ) : (
                <Grid container spacing={1}>
                {categories.map((cat) => {
                  const label = cat.category_label || cat.category_id || "Category";
                  const count = cat.case_count || 0;
                  const hintValue = cat.total_value || 0;
                  const frequency = Number(cat.frequency_percentage || 0);
                  const normalizedScore = frequency > 0 ? frequency : Math.min(100, count * 2);
                  const risk = normalizedScore >= 60 ? "CRITICAL" : normalizedScore >= 35 ? "HIGH" : normalizedScore >= 15 ? "MEDIUM" : "LOW";
                  const selected = selectedCategoryId === cat.category_id;
                  return (
                    <Grid key={label} item xs={12} sm={6}>
                      <Card
                          onClick={() => setSelectedCategoryId(cat.category_id)}
                          sx={{
                            cursor: "pointer",
                            border: selected ? "2px solid #1976d2" : "1px solid #e5e7eb",
                            background: selected ? "#eef6ff" : "#f9fafb",
                          }}
                        >
                          <CardContent sx={{ p: 1.2 }}>
                            <Typography variant="subtitle2" sx={{ color: "text.primary", fontWeight: 700 }}>
                              {label}
                            </Typography>
                            <Typography variant="caption" sx={{ color: "text.secondary", display: "block" }}>
                              Count: {count}
                            </Typography>
                            <Typography variant="caption" sx={{ color: "text.secondary", display: "block" }}>
                              Value: {money(hintValue)}
                            </Typography>
                            <Typography variant="caption" sx={{ color: "text.secondary", display: "block" }}>
                              Open/Closed: {cat.open_count ?? 0}/{cat.closed_count ?? 0}
                            </Typography>
                            <Chip size="small" color={riskColor(risk)} label={risk} sx={{ mt: 0.8 }} />
                          </CardContent>
                        </Card>
                      </Grid>
                    );
                  })}
                </Grid>
              )}
            </CardContent>
          </Card>

          <Card sx={{ border: "1px solid #e5e7eb" }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 1, color: "text.primary" }}>
                Records: {selectedCategoryObject?.category_label || selectedCategoryId}
              </Typography>
              {loadingRecords ? (
                <CircularProgress size={22} />
              ) : (
                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Invoice ID</TableCell>
                        <TableCell>Vendor</TableCell>
                        <TableCell>Amount</TableCell>
                        <TableCell>DPO / Days</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Badge</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {records.map((record) => {
                        const isSelected = selectedRecord?.exception_id === record.exception_id;
                        const dpo = record.avg_resolution_time_days ?? record.days_in_exception ?? record.avg_dpo ?? 0;
                        return (
                          <TableRow
                            key={record.exception_id}
                            hover
                            onClick={() => runAnalysis(record)}
                            sx={{
                              cursor: "pointer",
                              background: isSelected ? "#eef6ff" : "transparent",
                            }}
                          >
                            <TableCell>{record.invoice_id || record.exception_id}</TableCell>
                            <TableCell>{record.vendor_id || record.recurring_vendor_hint || "N/A"}</TableCell>
                            <TableCell>{money(record.invoice_amount || record.value_at_risk || 0)}</TableCell>
                            <TableCell>{Number(dpo || 0).toFixed(1)}</TableCell>
                            <TableCell>
                              <Chip size="small" label={record.exception_type || selectedCategoryObject?.category_id || "Exception"} />
                            </TableCell>
                            <TableCell>
                              <Chip size="small" label={quickBadge(record)} color="warning" />
                            </TableCell>
                          </TableRow>
                        );
                      })}
                      {records.length === 0 && (
                        <TableRow>
                          <TableCell colSpan={6}>
                            <Typography variant="body2" sx={{ color: "text.secondary" }}>
                              No records returned for this category.
                            </Typography>
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={7}>
          <Card sx={{ border: "1px solid #e5e7eb", mb: 2 }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 1, color: "text.primary" }}>
                AI Analysis
              </Typography>
              {!selectedRecord && (
                <Typography variant="body2" sx={{ color: "text.secondary" }}>
                  Select a record to generate analysis.
                </Typography>
              )}
              {loadingAnalysis && <CircularProgress size={24} />}
              {analysis && (
                <Stack spacing={1.2}>
                  <Typography variant="body2"><strong>Summary:</strong> {analysis.summary || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Happy Path:</strong> {analysis.happy_path?.path || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Exception Path:</strong> {analysis.exception_path?.path || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Root Cause:</strong> {analysis.root_cause_analysis?.most_likely_cause || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Why:</strong> {analysis.root_cause_analysis?.why || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Financial Impact:</strong> Value at Risk {money(analysis.financial_impact?.value_at_risk)} | Savings {money(analysis.financial_impact?.potential_savings)}</Typography>
                  <Typography variant="body2"><strong>Turnaround Risk:</strong> {analysis.turnaround_risk?.risk_level || "N/A"} (ETA {analysis.turnaround_risk?.estimated_processing_days ?? 0} days)</Typography>
                  <Typography variant="body2"><strong>Recommended Role:</strong> {analysis.recommended_resolution_role || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Automation Decision:</strong> {analysis.automation_decision || "N/A"}</Typography>
                  <Chip
                    label={`Celonis Evidence: ${analysis.root_cause_analysis?.celonis_evidence || "Available in analysis payload"}`}
                    sx={{ width: "fit-content", background: "#dcfce7", color: "#166534" }}
                  />

                  {analysis.exception_context_from_celonis && (
                    <Card sx={{ mt: 1, border: "1px solid #93c5fd", background: "#eff6ff" }}>
                      <CardContent sx={{ p: 1.2 }}>
                        <Typography variant="subtitle2" sx={{ color: "#1d4ed8", fontWeight: 700, mb: 0.6 }}>
                          Celonis Context Used
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#1e3a8a", mb: 0.6 }}>
                          {analysis.exception_context_from_celonis.category_summary}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#1e40af", mb: 0.3 }}>
                          Process Steps: {(analysis.exception_context_from_celonis.process_step_signals || []).slice(0, 2).join(" | ") || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#1e40af", mb: 0.3 }}>
                          Variants: {(analysis.exception_context_from_celonis.variant_signals || []).slice(0, 2).join(" | ") || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#1e40af", mb: 0.3 }}>
                          Resources: {(analysis.exception_context_from_celonis.resource_signals || []).slice(0, 2).join(" | ") || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#1e40af" }}>
                          Cycle Time: {(analysis.exception_context_from_celonis.cycle_time_signals || []).join(" | ") || "N/A"}
                        </Typography>
                      </CardContent>
                    </Card>
                  )}
                </Stack>
              )}
            </CardContent>
          </Card>

          <Card sx={{ border: "2px solid #1976d2", mb: 2, background: "#f5f9ff" }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 1, color: "#1976d2" }}>
                Next Best Action
              </Typography>
              {!nextAction ? (
                <Typography variant="body2" sx={{ color: "text.secondary" }}>
                  Run analysis to get next best action.
                </Typography>
              ) : (
                <Stack spacing={1}>
                  <Typography variant="body1"><strong>Action:</strong> {nextAction.action || analysis?.next_best_action?.action || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Why:</strong> {nextAction.why || analysis?.next_best_action?.why || "N/A"}</Typography>
                  <Typography variant="body2"><strong>Confidence:</strong> {Number(nextAction.confidence ?? analysis?.next_best_action?.confidence ?? 0).toFixed(2)}</Typography>
                  <Typography variant="body2"><strong>Suggested Owner:</strong> {analysis?.recommended_resolution_role || "N/A"}</Typography>
                  <Typography variant="body2"><strong>ETA / Urgency:</strong> {analysis?.turnaround_risk?.estimated_processing_days ?? 0} days / {analysis?.turnaround_risk?.risk_level || "N/A"}</Typography>

                  {Array.isArray(analysis?.next_best_actions) && analysis.next_best_actions.length > 0 && (
                    <Box sx={{ mt: 0.8 }}>
                      <Typography variant="subtitle2" sx={{ color: "#1d4ed8", fontWeight: 700, mb: 0.5 }}>
                        Process-Derived Action Options
                      </Typography>
                      {analysis.next_best_actions.slice(0, 3).map((item, idx) => (
                        <Typography key={idx} variant="caption" sx={{ display: "block", color: "#1e293b", mb: 0.4 }}>
                          {idx + 1}. {item.action} | {item.why}
                        </Typography>
                      ))}
                    </Box>
                  )}

                  {analysis?.prompt_for_next_agents && (
                    <Card sx={{ mt: 1, border: "1px solid #f59e0b", background: "#fffbeb" }}>
                      <CardContent sx={{ p: 1.2 }}>
                        <Typography variant="subtitle2" sx={{ color: "#b45309", fontWeight: 700, mb: 0.5 }}>
                          Prompt Package for Next Agents
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#92400e", mb: 0.4 }}>
                          Targets: {(analysis.prompt_for_next_agents.target_agents || []).join(", ") || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#92400e", mb: 0.4 }}>
                          Intent: {analysis.prompt_for_next_agents.handoff_intent || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#92400e", mb: 0.4 }}>
                          Prompt: {analysis.prompt_for_next_agents.execution_prompt || "N/A"}
                        </Typography>
                        <Typography variant="caption" sx={{ display: "block", color: "#92400e" }}>
                          PI Rationale: {analysis.prompt_for_next_agents.pi_rationale || "N/A"}
                        </Typography>
                      </CardContent>
                    </Card>
                  )}
                </Stack>
              )}
            </CardContent>
          </Card>

          {analysis?.send_to_human_review && (
            <Card sx={{ border: "1px solid #ed6c02", background: "#fff7ed" }}>
              <CardContent>
                <Typography variant="h6" sx={{ color: "#ed6c02", mb: 1 }}>
                  Human Review
                </Typography>
                <Typography variant="body2" sx={{ color: "text.secondary", mb: 1.2 }}>
                  This case is flagged for human review.
                </Typography>
                <Button variant="contained" onClick={sendToTeams} disabled={loadingTeams}>
                  {loadingTeams ? "Sending..." : "Send to Microsoft Teams for Review"}
                </Button>
              </CardContent>
            </Card>
          )}
        </Grid>
      </Grid>

      <Snackbar
        open={toast.open}
        autoHideDuration={3500}
        onClose={() => setToast((t) => ({ ...t, open: false }))}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      >
        <Alert severity={toast.severity} onClose={() => setToast((t) => ({ ...t, open: false }))}>
          {toast.message}
        </Alert>
      </Snackbar>
    </div>
  );
}
