import React, { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Box, Card, CardContent, Chip, CircularProgress, Divider, Grid, Stack, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from "@mui/material";
import LoadingSpinner from "../components/LoadingSpinner";
import { analyzeExceptionRecord, fetchExceptionCategories, fetchExceptionRecords, waitForCacheReady } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const pickData = (r) => {
  if (!r) return null;
  if (r.data !== undefined) return r.data;
  return r;
};

const money = (v) => {
  const n = Number(v || 0);
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M $`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K $`;
  return `${n.toFixed(0)} $`;
};

const pct = (v) => `${Number(v || 0).toFixed(1)}%`;

const riskTone = (risk) => {
  const value = String(risk || "").toUpperCase();
  if (value === "CRITICAL") return { bg: "#FAEAEA", border: "#E0A0A0", color: "#B03030" };
  if (value === "HIGH") return { bg: "#FEF3DC", border: "#F0C870", color: "#A05A10" };
  if (value === "MEDIUM") return { bg: "#EBF2FC", border: "#90B8E8", color: "#1E4E8C" };
  return { bg: "#E0F0E8", border: "#80C0A0", color: "#1D5C3A" };
};

function MetricCard({ label, value, note, color = "#1E4E8C" }) {
  return (
    <Card sx={{ height: "100%" }}>
      <CardContent>
        <Typography sx={{ fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 1 }}>{label}</Typography>
        <Typography sx={{ fontFamily: S, fontSize: "2rem", color, lineHeight: 1.1, mb: 0.5 }}>{value}</Typography>
        <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>{note}</Typography>
      </CardContent>
    </Card>
  );
}

function AnalysisCard({ title, color, bg, border, children }) {
  return (
    <Card sx={{ height: "100%", background: `${bg} !important`, border: `1px solid ${border} !important`, borderLeft: `3px solid ${color} !important` }}>
      <CardContent>
        <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color, mb: 1.2 }}>{title}</Typography>
        {children}
      </CardContent>
    </Card>
  );
}

function LabelValue({ label, value }) {
  return (
    <Box sx={{ py: 0.7, borderBottom: "1px solid #E8E3DA" }}>
      <Typography sx={{ fontSize: "0.68rem", textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.35 }}>{label}</Typography>
      <Typography sx={{ fontSize: "0.8rem", color: "#5C5650", fontFamily: G, lineHeight: 1.55 }}>{value || "N/A"}</Typography>
    </Box>
  );
}

export default function ExceptionIntelligence() {
  const [loading, setLoading] = useState(true);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [error, setError] = useState("");
  const [records, setRecords] = useState([]);
  const [selectedRecordId, setSelectedRecordId] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const analysisRequestRef = useRef(0);

  useEffect(() => {
    let active = true;
    (async () => {
      setLoading(true);
      setError("");
      try {
        let categoriesRes = await fetchExceptionCategories();
        let categories = (Array.isArray(pickData(categoriesRes)) ? pickData(categoriesRes) : [])
          .filter((row) => Number(row.case_count || 0) > 0)
          .sort((a, b) => Number(b.case_count || 0) - Number(a.case_count || 0));
        if (categories.length === 0) {
          await waitForCacheReady();
          categoriesRes = await fetchExceptionCategories();
          categories = (Array.isArray(pickData(categoriesRes)) ? pickData(categoriesRes) : [])
            .filter((row) => Number(row.case_count || 0) > 0)
            .sort((a, b) => Number(b.case_count || 0) - Number(a.case_count || 0));
        }

        const recordGroups = await Promise.all(
          categories.map(async (category) => {
            const res = await fetchExceptionRecords(category.category_id);
            const rows = Array.isArray(pickData(res)) ? pickData(res) : [];
            return rows.map((row) => ({
              ...row,
              category_id: category.category_id,
              category_label: category.category_label,
            }));
          }),
        );

        const flattened = recordGroups
          .flat()
          .sort((a, b) => Number(b.invoice_amount || b.value_at_risk || 0) - Number(a.invoice_amount || a.value_at_risk || 0));

        if (!active) return;
        setRecords(flattened);
        const first = flattened[0];
        if (first) {
          setSelectedRecordId(first.exception_id);
        }
      } catch (e) {
        if (!active) return;
        setError(e?.response?.data?.detail || e.message || "Failed to load exception intelligence.");
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => { active = false; };
  }, []);

  const selectedRecord = useMemo(
    () => records.find((row) => row.exception_id === selectedRecordId) || null,
    [records, selectedRecordId],
  );

  useEffect(() => {
    if (!selectedRecord) return;
    let active = true;
    (async () => {
      const requestId = ++analysisRequestRef.current;
      setAnalysisLoading(true);
      try {
        const payload = {
          exception_type: selectedRecord.category_label || selectedRecord.exception_type,
          exception_id: selectedRecord.exception_id,
          invoice_id: selectedRecord.invoice_id || selectedRecord.document_number || selectedRecord.case_id || "",
          vendor_id: selectedRecord.vendor_id || "",
          vendor_name: selectedRecord.vendor_name || "",
          invoice_amount: selectedRecord.invoice_amount || selectedRecord.value_at_risk || 0,
          currency: selectedRecord.currency || "USD",
          days_until_due: selectedRecord.days_until_due || 0,
          extra_context: selectedRecord,
        };
        const analysisData = pickData(await analyzeExceptionRecord(payload));
        if (!active || analysisRequestRef.current !== requestId) return;
        setAnalysis(analysisData);
      } catch (e) {
        if (!active || analysisRequestRef.current !== requestId) return;
        setError(e?.response?.data?.detail || e.message || "Failed to analyze selected exception.");
      } finally {
        if (active && analysisRequestRef.current === requestId) setAnalysisLoading(false);
      }
    })();
    return () => { active = false; };
  }, [selectedRecord]);

  const totals = useMemo(() => {
    const totalValue = records.reduce((sum, row) => sum + Number(row.invoice_amount || row.value_at_risk || 0), 0);
    const categoryCount = new Set(records.map((row) => row.category_id)).size;
    const autoCandidates = records.filter((row) => String(row.risk_level || "").toUpperCase() !== "CRITICAL").length;
    return { totalValue, categoryCount, autoCandidates };
  }, [records]);

  if (loading) return <LoadingSpinner message="Building exception intelligence from Celonis records..." />;

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", mb: 0.5 }}>
          Exception Intelligence
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Review each exception with AI-generated happy path, exception path, next best action, and classifier verdict.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}>
          <MetricCard label="Exceptions" value={records.length} note="Actionable exception records loaded" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard label="Categories" value={totals.categoryCount} note="Distinct mined exception buckets" color="#A05A10" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard label="Value at Risk" value={money(totals.totalValue)} note="Invoice exposure represented here" color="#B03030" />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard label="Auto Candidates" value={totals.autoCandidates} note="Non-critical cases likely fit for automation" color="#1A6B5E" />
        </Grid>
      </Grid>

      <Grid container spacing={2.5}>
        <Grid item xs={12} md={5}>
          <Card sx={{ height: "100%" }}>
            <CardContent>
              <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 1.5, gap: 1 }}>
                <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F" }}>Exception Queue</Typography>
                <Chip size="small" label={`${records.length} records`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }} />
              </Box>
              <TableContainer sx={{ maxHeight: 780 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      {["Exception", "Invoice", "Vendor", "Risk", "Value"].map((header) => <TableCell key={header}>{header}</TableCell>)}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {records.map((row) => {
                      const selected = row.exception_id === selectedRecordId;
                      const tone = riskTone(row.risk_level);
                      return (
                        <TableRow
                          key={row.exception_id}
                          hover
                          onClick={() => setSelectedRecordId(row.exception_id)}
                          sx={{ cursor: "pointer", background: selected ? "#F5ECD9 !important" : "transparent" }}
                        >
                          <TableCell>
                            <Typography sx={{ fontSize: "0.8rem", color: selected ? "#B5742A" : "#17140F", fontWeight: 600, fontFamily: G }}>{row.category_label || row.exception_type}</Typography>
                            <Typography sx={{ fontSize: "0.7rem", color: "#9C9690", fontFamily: G }}>{row.exception_id}</Typography>
                          </TableCell>
                          <TableCell>{row.invoice_id || row.document_number || row.case_id || "N/A"}</TableCell>
                          <TableCell>{row.vendor_id || "N/A"}</TableCell>
                          <TableCell>
                            <Box sx={{ display: "inline-block", background: tone.bg, color: tone.color, border: `1px solid ${tone.border}`, px: 0.8, py: 0.2, borderRadius: "99px" }}>
                              <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, fontFamily: G }}>{row.risk_level || "MEDIUM"}</Typography>
                            </Box>
                          </TableCell>
                          <TableCell>{money(row.invoice_amount || row.value_at_risk)}</TableCell>
                        </TableRow>
                      );
                    })}
                    {records.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={5}>
                          <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>No exception records are available yet.</Typography>
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={7}>
          {!selectedRecord ? (
            <Card>
              <CardContent>
                <Typography sx={{ fontSize: "0.9rem", color: "#9C9690", fontFamily: G }}>Select an exception to inspect its AI analysis.</Typography>
              </CardContent>
            </Card>
          ) : analysisLoading ? (
            <LoadingSpinner message="Analyzing selected exception with process context..." />
          ) : (
            <Stack spacing={2}>
              <Card>
                <CardContent>
                  <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2, flexWrap: "wrap", mb: 1.5 }}>
                    <Box>
                      <Typography sx={{ fontFamily: S, fontSize: "1.25rem", color: "#17140F" }}>{selectedRecord.category_label || selectedRecord.exception_type}</Typography>
                      <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>
                        Invoice {selectedRecord.invoice_id || selectedRecord.document_number || selectedRecord.case_id || "N/A"} · Vendor {selectedRecord.vendor_id || "N/A"}
                      </Typography>
                    </Box>
                    <Stack direction="row" spacing={1} flexWrap="wrap">
                      <Chip size="small" label={`Value ${money(selectedRecord.invoice_amount || selectedRecord.value_at_risk)}`} color="warning" />
                      <Chip size="small" label={`DPO ${Number(selectedRecord.dpo || selectedRecord.actual_dpo || 0).toFixed(1)}d`} sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" }} />
                      <Chip size="small" label={`Frequency ${pct(selectedRecord.frequency_percentage)}`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }} />
                    </Stack>
                  </Box>
                  <Typography sx={{ fontSize: "0.84rem", color: "#5C5650", fontFamily: G, lineHeight: 1.6 }}>
                    {analysis?.summary || "AI summary unavailable for this record."}
                  </Typography>
                </CardContent>
              </Card>

              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <AnalysisCard title="Happy Path" color="#1A6B5E" bg="#F7FBF9" border="#DCF0EB">
                    <LabelValue label="Path" value={analysis?.happy_path?.path} />
                    <LabelValue label="Average Duration" value={`${Number(analysis?.happy_path?.avg_duration_days || 0).toFixed(1)} days`} />
                    <LabelValue label="Why It Matters" value="Use this as the benchmark flow the exception should return to." />
                  </AnalysisCard>
                </Grid>
                <Grid item xs={12} md={6}>
                  <AnalysisCard title="Exception Path" color="#B03030" bg="#FDF7F7" border="#FAEAEA">
                    <LabelValue label="Observed Path" value={analysis?.exception_path?.path || selectedRecord.summary} />
                    <LabelValue label="Exception Stage" value={analysis?.exception_path?.exception_stage} />
                    <LabelValue label="Extra Delay" value={`${Number(analysis?.exception_path?.extra_days || 0).toFixed(1)} days`} />
                  </AnalysisCard>
                </Grid>
              </Grid>

              <Grid container spacing={2}>
                <Grid item xs={12} md={7}>
                  <AnalysisCard title="Next Best Action" color="#A05A10" bg="#FEF3DC" border="#F0C870">
                    <LabelValue label="Recommended Action" value={analysis?.next_best_action?.action} />
                    <LabelValue label="Why" value={analysis?.next_best_action?.why} />
                    <LabelValue label="Confidence" value={Number(analysis?.next_best_action?.confidence || 0).toFixed(2)} />
                    <Divider sx={{ my: 1.2 }} />
                    <Typography sx={{ fontSize: "0.68rem", textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>Root Cause</Typography>
                    <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.5 }}>
                      <strong>{analysis?.root_cause_analysis?.most_likely_cause || "N/A"}</strong>
                    </Typography>
                    <Typography sx={{ fontSize: "0.78rem", color: "#7A746E", fontFamily: G, lineHeight: 1.55 }}>
                      {analysis?.root_cause_analysis?.why || analysis?.root_cause_analysis?.celonis_evidence || "No rationale returned."}
                    </Typography>
                  </AnalysisCard>
                </Grid>
                <Grid item xs={12} md={5}>
                  <AnalysisCard title="Classifier Agent" color="#1E4E8C" bg="#EBF2FC" border="#90B8E8">
                    <LabelValue label="Decision" value={analysis?.classifier_agent?.decision || analysis?.automation_decision} />
                    <LabelValue label="Mode" value={analysis?.classifier_agent?.recommended_mode} />
                    <LabelValue label="Owner" value={analysis?.vendor_name || analysis?.vendor_id || selectedRecord?.vendor_name || selectedRecord?.vendor_id || selectedRecord?.recurring_vendor_hint} />
                    <LabelValue label="Rationale" value={analysis?.classifier_agent?.rationale} />
                    <LabelValue label="Confidence" value={Number(analysis?.classifier_agent?.confidence || 0).toFixed(2)} />
                  </AnalysisCard>
                </Grid>
              </Grid>

              <Card>
                <CardContent>
                  <Typography sx={{ fontFamily: S, fontSize: "1.05rem", color: "#17140F", mb: 1.2 }}>Celonis Signals</Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={4}>
                      <LabelValue label="Process Steps" value={(analysis?.exception_context_from_celonis?.process_step_signals || []).join(" | ")} />
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <LabelValue label="Variants" value={(analysis?.exception_context_from_celonis?.variant_signals || []).join(" | ")} />
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <LabelValue label="Cycle Time" value={(analysis?.exception_context_from_celonis?.cycle_time_signals || []).join(" | ")} />
                    </Grid>
                  </Grid>
                </CardContent>
              </Card>
            </Stack>
          )}
        </Grid>
      </Grid>
    </div>
  );
}
