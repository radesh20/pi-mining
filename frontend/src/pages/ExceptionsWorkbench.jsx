import React, { useEffect, useMemo, useState } from "react";
import { Alert, Box, Button, Card, CardContent, Chip, CircularProgress, Grid, Snackbar, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from "@mui/material";
import { analyzeExceptionRecord, fetchExceptionCategories, fetchExceptionRecords, fetchNextBestAction, sendExceptionToTeams } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const money = (v) => { const n = Number(v || 0); if (!n) return "N/A"; if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`; if (n >= 1000) return `${(n / 1000).toFixed(1)}K $`; return `${n.toFixed(0)} $`; };
const pickData = (r) => { if (!r) return null; if (r.data !== undefined) return r.data; return r; };
const riskColor = (r) => { const v = String(r || "").toUpperCase(); if (v === "CRITICAL") return "error"; if (v === "HIGH" || v === "MEDIUM") return "warning"; return "success"; };
const quickBadge = (rec) => { const t = `${rec.summary || ""} ${rec.exception_type || ""}`.toLowerCase(); if (t.includes("mismatch")) return "Mismatch"; if (t.includes("late") || t.includes("due")) return "Late"; if (t.includes("early") || t.includes("optim")) return "Optimization"; if (t.includes("short")) return "Short Terms"; return "Exception"; };

function RiskBadge({ risk }) {
  const map = { CRITICAL: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0" }, HIGH: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" }, MEDIUM: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" }, LOW: { bg: "#E0F0E8", color: "#1D5C3A", border: "#80C0A0" } };
  const style = map[String(risk).toUpperCase()] || map.LOW;
  return (
    <Box sx={{ display: "inline-block", background: style.bg, color: style.color, border: `1px solid ${style.border}`, px: 1, py: 0.2, borderRadius: "99px" }}>
      <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, fontFamily: G, letterSpacing: "0.04em" }}>{risk}</Typography>
    </Box>
  );
}

function SectionLabel({ children }) {
  return <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 1 }}>{children}</Typography>;
}

function InfoRow({ label, value }) {
  return (
    <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", py: 0.7, borderBottom: "1px solid #F0EDE6" }}>
      <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, flexShrink: 0, mr: 2 }}>{label}</Typography>
      <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G, textAlign: "right" }}>{value || "N/A"}</Typography>
    </Box>
  );
}

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
      .then((res) => { const rows = Array.isArray(pickData(res)) ? pickData(res) : []; if (!active) return; setCategories(rows); if (rows.length > 0) setSelectedCategoryId(rows[0].category_id || ""); })
      .catch((e) => { if (!active) return; setError(e?.response?.data?.detail || e.message || "Failed to load categories"); })
      .finally(() => { if (active) setLoadingCategories(false); });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!selectedCategoryId) return;
    setLoadingRecords(true); setRecords([]); setSelectedRecord(null); setAnalysis(null); setNextAction(null);
    fetchExceptionRecords(selectedCategoryId)
      .then((res) => setRecords(Array.isArray(pickData(res)) ? pickData(res) : []))
      .catch((e) => setError(e?.response?.data?.detail || e.message || "Failed to load records"))
      .finally(() => setLoadingRecords(false));
  }, [selectedCategoryId]);

  const selectedCategoryObject = useMemo(() => categories.find(c => c.category_id === selectedCategoryId), [categories, selectedCategoryId]);

  const runAnalysis = async (record) => {
    setLoadingAnalysis(true); setSelectedRecord(record); setAnalysis(null); setNextAction(null);
    try {
      const payload = { exception_type: selectedCategoryObject?.category_label || selectedCategoryId, exception_id: record.exception_id, invoice_id: record.invoice_id || "", vendor_id: record.vendor_id || record.recurring_vendor_hint || "", vendor_name: record.vendor_name || "", invoice_amount: record.invoice_amount || 0, currency: record.currency || "USD", extra_context: record };
      const aData = pickData(await analyzeExceptionRecord(payload));
      setAnalysis(aData);
      setNextAction(pickData(await fetchNextBestAction(aData)));
    } catch (e) { setError(e?.response?.data?.detail || e.message || "Failed to analyze"); }
    finally { setLoadingAnalysis(false); }
  };

  const sendToTeams = async () => {
    if (!analysis) return;
    setLoadingTeams(true);
    try {
      const d = pickData(await sendExceptionToTeams(analysis));
      setToast({ open: true, message: d?.success ? "Sent to Microsoft Teams successfully." : "Failed to send to Teams.", severity: d?.success ? "success" : "error" });
    } catch (e) { setToast({ open: true, message: e?.response?.data?.detail || e.message || "Failed.", severity: "error" }); }
    finally { setLoadingTeams(false); }
  };

  return (
    <div className="page-container">
      {/* Header */}
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Exceptions Workbench
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Analyze exceptions, predict next best action, and escalate to Teams when human review is required.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={2.5}>
        {/* LEFT PANEL */}
        <Grid item xs={12} md={5}>
          {/* Category Cards */}
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 1.5 }}>Exception Categories</Typography>
              {loadingCategories ? <CircularProgress size={20} /> : (
                <Grid container spacing={1}>
                  {categories.map((cat) => {
                    const label = cat.category_label || cat.category_id || "Category";
                    const count = cat.case_count || 0;
                    const freq = Number(cat.frequency_percentage || 0);
                    const score = freq > 0 ? freq : Math.min(100, count * 2);
                    const risk = score >= 60 ? "CRITICAL" : score >= 35 ? "HIGH" : score >= 15 ? "MEDIUM" : "LOW";
                    const selected = selectedCategoryId === cat.category_id;
                    return (
                      <Grid item xs={12} sm={6} key={label}>
                        <Box
                          onClick={() => setSelectedCategoryId(cat.category_id)}
                          sx={{ p: 1.5, borderRadius: "10px", border: selected ? "2px solid #B5742A" : "1px solid #E8E3DA", background: selected ? "#F5ECD9" : "#FDFCFA", cursor: "pointer", transition: "all 0.15s", "&:hover": { background: selected ? "#F5ECD9" : "#F5F2EC", borderColor: selected ? "#B5742A" : "#C4BDB0" } }}
                        >
                          <Typography sx={{ fontWeight: 600, fontSize: "0.82rem", color: "#17140F", mb: 0.4, fontFamily: G }}>{label}</Typography>
                          <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G }}>Count: {count}</Typography>
                          <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G, mb: 0.8 }}>Value: {money(cat.total_value)}</Typography>
                          <RiskBadge risk={risk} />
                        </Box>
                      </Grid>
                    );
                  })}
                </Grid>
              )}
            </CardContent>
          </Card>

          {/* Records Table */}
          <Card>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.5 }}>
                Records: <span style={{ color: "#B5742A" }}>{selectedCategoryObject?.category_label || selectedCategoryId}</span>
              </Typography>
              {loadingRecords ? <CircularProgress size={20} /> : (
                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        {["Invoice", "Vendor", "Amount", "DPO", "Tag"].map(h => <TableCell key={h}>{h}</TableCell>)}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {records.map((rec) => {
                        const isSel = selectedRecord?.exception_id === rec.exception_id;
                        const dpo = rec.avg_resolution_time_days ?? rec.days_in_exception ?? rec.avg_dpo ?? 0;
                        return (
                          <TableRow key={rec.exception_id} hover onClick={() => runAnalysis(rec)} sx={{ cursor: "pointer", background: isSel ? "#F5ECD9 !important" : "transparent" }}>
                            <TableCell sx={{ fontWeight: isSel ? 600 : 400, color: isSel ? "#B5742A !important" : "inherit" }}>{rec.invoice_id || rec.exception_id}</TableCell>
                            <TableCell>{rec.vendor_id || rec.recurring_vendor_hint || "—"}</TableCell>
                            <TableCell>{money(rec.invoice_amount || rec.value_at_risk || 0)}</TableCell>
                            <TableCell>{Number(dpo || 0).toFixed(1)}</TableCell>
                            <TableCell><Box sx={{ background: "#FEF3DC", color: "#A05A10", border: "1px solid #F0C870", px: 0.8, py: 0.1, borderRadius: "99px", fontSize: "0.65rem", fontWeight: 600, fontFamily: G, display: "inline-block" }}>{quickBadge(rec)}</Box></TableCell>
                          </TableRow>
                        );
                      })}
                      {records.length === 0 && <TableRow><TableCell colSpan={5}><Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>No records for this category.</Typography></TableCell></TableRow>}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* RIGHT PANEL */}
        <Grid item xs={12} md={7}>
          {/* AI Analysis */}
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#17140F", mb: 1.5 }}>AI Analysis</Typography>
              {!selectedRecord && !loadingAnalysis && (
                <Box sx={{ py: 3, textAlign: "center" }}>
                  <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>Select a record to generate analysis.</Typography>
                </Box>
              )}
              {loadingAnalysis && <Box sx={{ display: "flex", gap: 2, alignItems: "center", py: 2 }}><CircularProgress size={20} /><Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>Analyzing with Celonis context…</Typography></Box>}
              {analysis && (
                <Stack spacing={0}>
                  <InfoRow label="Summary" value={analysis.summary} />
                  <InfoRow label="Happy Path" value={analysis.happy_path?.path} />
                  <InfoRow label="Exception Path" value={analysis.exception_path?.path} />
                  <InfoRow label="Root Cause" value={analysis.root_cause_analysis?.most_likely_cause} />
                  <InfoRow label="Financial Impact" value={`Risk: ${money(analysis.financial_impact?.value_at_risk)} · Savings: ${money(analysis.financial_impact?.potential_savings)}`} />
                  <InfoRow label="Turnaround Risk" value={`${analysis.turnaround_risk?.risk_level || "N/A"} · ETA ${analysis.turnaround_risk?.estimated_processing_days ?? 0} days`} />
                  <InfoRow label="Recommended Role" value={analysis.recommended_resolution_role} />
                  <InfoRow label="Automation Decision" value={analysis.automation_decision} />
                  <Box sx={{ pt: 1 }}>
                    <span className="evidence-tag">Celonis Evidence: {analysis.root_cause_analysis?.celonis_evidence || "Available"}</span>
                  </Box>

                  {analysis.exception_context_from_celonis && (
                    <Box sx={{ mt: 1.5, background: "#EBF2FC", border: "1px solid #90B8E8", borderLeft: "3px solid #1E4E8C", borderRadius: "0 10px 10px 0", p: 1.5 }}>
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#1E4E8C", textTransform: "uppercase", letterSpacing: "0.05em", fontFamily: G, mb: 0.8 }}>Celonis Context Used</Typography>
                      <Typography sx={{ fontSize: "0.78rem", color: "#1E3A6B", mb: 0.6, fontFamily: G }}>{analysis.exception_context_from_celonis.category_summary}</Typography>
                      {[["Process Steps", analysis.exception_context_from_celonis.process_step_signals], ["Variants", analysis.exception_context_from_celonis.variant_signals], ["Cycle Time", analysis.exception_context_from_celonis.cycle_time_signals]].map(([k, v]) => v?.length ? (
                        <Typography key={k} sx={{ fontSize: "0.72rem", color: "#2E5090", fontFamily: G, mb: 0.3 }}><strong>{k}:</strong> {(v || []).slice(0, 2).join(" | ")}</Typography>
                      ) : null)}
                    </Box>
                  )}
                </Stack>
              )}
            </CardContent>
          </Card>

          {/* Next Best Action */}
          <Card sx={{ mb: 2, border: "1px solid #DEC48A !important" }}>
            <CardContent>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1.5 }}>
                <Box sx={{ width: "8px", height: "8px", borderRadius: "50%", background: "#B5742A" }} />
                <Typography sx={{ fontFamily: S, fontSize: "1.15rem", color: "#B5742A" }}>Next Best Action</Typography>
              </Box>
              {!nextAction ? (
                <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>Run analysis to get next best action.</Typography>
              ) : (
                <Stack spacing={0}>
                  <InfoRow label="Action" value={nextAction.action || analysis?.next_best_action?.action} />
                  <InfoRow label="Why" value={nextAction.why || analysis?.next_best_action?.why} />
                  <InfoRow label="Confidence" value={Number(nextAction.confidence ?? analysis?.next_best_action?.confidence ?? 0).toFixed(2)} />
                  <InfoRow label="Owner" value={analysis?.recommended_resolution_role} />
                  <InfoRow label="ETA / Urgency" value={`${analysis?.turnaround_risk?.estimated_processing_days ?? 0} days / ${analysis?.turnaround_risk?.risk_level || "N/A"}`} />

                  {Array.isArray(analysis?.next_best_actions) && analysis.next_best_actions.length > 0 && (
                    <Box sx={{ mt: 1.5 }}>
                      <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>Process-Derived Options</Typography>
                      {analysis.next_best_actions.slice(0, 3).map((item, idx) => (
                        <Box key={idx} sx={{ display: "flex", gap: 1, mb: 0.8 }}>
                          <Box sx={{ background: "#F0EDE6", color: "#9C9690", width: "18px", height: "18px", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "0.65rem", fontWeight: 700, flexShrink: 0, fontFamily: G }}>
                            {idx + 1}
                          </Box>
                          <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G }}>{item.action} — {item.why}</Typography>
                        </Box>
                      ))}
                    </Box>
                  )}

                  {analysis?.prompt_for_next_agents && (
                    <Box sx={{ mt: 1.5, background: "#FEF3DC", border: "1px solid #F0C870", borderLeft: "3px solid #A05A10", borderRadius: "0 10px 10px 0", p: 1.5 }}>
                      <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#A05A10", fontFamily: G, mb: 0.8 }}>Prompt Package for Next Agents</Typography>
                      {[["Targets", (analysis.prompt_for_next_agents.target_agents || []).join(", ")], ["Intent", analysis.prompt_for_next_agents.handoff_intent], ["PI Rationale", analysis.prompt_for_next_agents.pi_rationale]].map(([k, v]) => (
                        <Typography key={k} sx={{ fontSize: "0.72rem", color: "#92400E", fontFamily: G, mb: 0.4 }}><strong>{k}:</strong> {v || "N/A"}</Typography>
                      ))}
                    </Box>
                  )}
                </Stack>
              )}
            </CardContent>
          </Card>

          {/* Human Review */}
          {analysis?.send_to_human_review && (
            <Card sx={{ border: "1px solid #F0C870 !important", background: "#FEFCF5 !important" }}>
              <CardContent>
                <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                  <Box>
                    <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#A05A10", mb: 0.4 }}>Human Review Required</Typography>
                    <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>This case has been flagged for human review.</Typography>
                  </Box>
                  <Button variant="contained" onClick={sendToTeams} disabled={loadingTeams} sx={{ background: "#B5742A !important" }}>
                    {loadingTeams ? "Sending…" : "Send to Microsoft Teams"}
                  </Button>
                </Box>
              </CardContent>
            </Card>
          )}
        </Grid>
      </Grid>

      <Snackbar open={toast.open} autoHideDuration={3500} onClose={() => setToast(t => ({ ...t, open: false }))} anchorOrigin={{ vertical: "bottom", horizontal: "right" }}>
        <Alert severity={toast.severity} onClose={() => setToast(t => ({ ...t, open: false }))}>{toast.message}</Alert>
      </Snackbar>
    </div>
  );
}