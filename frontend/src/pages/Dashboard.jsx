import React, { useEffect, useState } from "react";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Grid from "@mui/material/Grid";
import Chip from "@mui/material/Chip";
import Button from "@mui/material/Button";
import Alert from "@mui/material/Alert";
import Stack from "@mui/material/Stack";
import ProcessMetrics from "../components/ProcessMetrics";
import LoadingSpinner from "../components/LoadingSpinner";
import ProcessTrajectoryView from "../components/ProcessTrajectoryView";
import { fetchCelonisContextLayer, fetchContextCoverage, fetchProcessInsights, refreshCache, validateWcmContext, waitForCacheReady } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

function SectionHeader({ label, meta }) {
  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 2, mt: 3.5 }}>
      <Typography sx={{ fontFamily: S, fontSize: "1.35rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.015em", whiteSpace: "nowrap" }}>
        {label}
      </Typography>
      {meta && <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "99px", px: 1.2, py: 0.2 }}>
        <Typography sx={{ fontSize: "0.69rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{meta}</Typography>
      </Box>}
      <Box sx={{ flex: 1, height: "1px", background: "#E8E3DA" }} />
    </Box>
  );
}

export default function Dashboard() {
  const [context, setContext] = useState(null);
  const [coverage, setCoverage] = useState(null);
  const [contextLayer, setContextLayer] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState("");
  const [validationMessage, setValidationMessage] = useState("");
  const [error, setError] = useState(null);
  const [cacheSlowMessage, setCacheSlowMessage] = useState("");

  const hasUsableAnalytics = (insights, coverageData, layer) => {
    const totalCases = Number(insights?.data?.total_cases ?? 0);
    const coverageCases = Number(coverageData?.data?.coverage?.total_cases ?? 0);
    const contextReady = Boolean(layer?.data?.context_ready);
    return totalCases > 0 || coverageCases > 0 || contextReady;
  };

  const loadAll = async (retryIfCacheCold = true, signal) => {
    const [insightsRes, coverageRes, layerRes] = await Promise.all([fetchProcessInsights(), fetchContextCoverage(), fetchCelonisContextLayer()]);
    if (retryIfCacheCold && !hasUsableAnalytics(insightsRes, coverageRes, layerRes)) {
      try {
        await waitForCacheReady({ signal });
        return await loadAll(false, signal);
      } catch (e) {
        if (e?.name === "AbortError") throw e;
        if (String(e?.message || "") === "Cache loading taking longer than expected") {
          setCacheSlowMessage("Cache loading taking longer than expected");
        }
        // Fall through and render the best available snapshot.
      }
    }
    setContext(insightsRes.data);
    setCoverage(coverageRes.data || null);
    setContextLayer(layerRes.data || insightsRes.data?.celonis_context_layer || null);
  };

  useEffect(() => {
    let active = true;
    const abortController = new AbortController();
    loadAll(true, abortController.signal)
      .catch((e) => {
        if (!active || e?.name === "AbortError") return;
        setError(e.response?.data?.detail || e.message);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      abortController.abort();
    };
  }, []);

  const handleRefresh = async () => {
    setRefreshing(true); setRefreshMessage(""); setError(null);
    try { await refreshCache(); await loadAll(); setRefreshMessage("Cache refreshed and coverage reloaded."); }
    catch (e) { setError(e.response?.data?.detail || e.message || "Refresh failed"); }
    finally { setRefreshing(false); }
  };

  const handleValidate = async () => {
    setValidationMessage(""); setError(null);
    try {
      const res = await validateWcmContext();
      const d = res.data || {};
      setValidationMessage(d.overall_passed ? "WCM validation passed." : `WCM validation found issues: ${(d.recommendations || [])[0] || "Mapping/coverage gaps."}`);
    } catch (e) { setError(e.response?.data?.detail || e.message || "Validation failed"); }
  };

  if (loading) return <LoadingSpinner message="Extracting process insights from Celonis..." />;
  if (error) return (
    <div className="page-container">
      <Box sx={{ pt: 4 }}>
        <div className="error-box">
          <Typography sx={{ fontFamily: S, fontSize: "1.1rem", mb: 0.5 }}>Celonis Connection Required</Typography>
          <Typography sx={{ fontSize: "0.875rem", mb: 1 }}>{error}</Typography>
          <Typography sx={{ fontSize: "0.82rem" }}>Go to Celonis Setup page to verify connection.</Typography>
        </div>
      </Box>
    </div>
  );

  return (
    <div className="page-container">
      {/* Header */}
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
          <Box>
            <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.5 }}>
              Process Mining → AI Agents
            </Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
              All data sourced live from Celonis Process Mining
            </Typography>
          </Box>
          <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" gap={1}>
            <Button variant="contained" size="small" onClick={handleRefresh} disabled={refreshing}>
              {refreshing ? "Refreshing…" : "Refresh Cache + Coverage"}
            </Button>
            <Button variant="outlined" size="small" onClick={handleValidate} disabled={refreshing}>
              Validate WCM Context
            </Button>
            {coverage?.refresh?.last_refreshed_at && (
              <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G }}>
                Last refresh: {coverage.refresh.last_refreshed_at}
              </Typography>
            )}
          </Stack>
        </Box>
        {refreshMessage && <Alert severity="success" sx={{ mt: 2 }}>{refreshMessage}</Alert>}
        {validationMessage && <Alert severity={validationMessage.includes("issues") ? "warning" : "success"} sx={{ mt: 2 }}>{validationMessage}</Alert>}
        {cacheSlowMessage && <Alert severity="warning" sx={{ mt: 2 }}>{cacheSlowMessage}</Alert>}
      </Box>

      <ProcessMetrics context={context} />

      {/* WCM Coverage — light */}
      {coverage && (
        <>
          <SectionHeader label="WCM Context Coverage" />
          <Card sx={{ background: "#FFFFFF !important", border: "1px solid #E8E3DA !important" }}>
            <CardContent>
              <Grid container spacing={3}>
                {[
                  { label: "Cases", value: coverage.coverage?.total_cases ?? 0, sub: `${coverage.coverage?.total_events ?? 0} events`, color: "#17140F" },
                  { label: "Exception Categories", value: coverage.coverage?.exception_category_count ?? 0, sub: `${coverage.coverage?.exception_record_count ?? 0} records`, color: "#17140F" },
                  { label: "OLAP Rows", value: coverage.coverage?.olap_rows ?? 0, sub: `${coverage.coverage?.tables_extracted ?? 0} tables`, color: "#17140F" },
                ].map((s) => (
                  <Grid item xs={12} sm={4} key={s.label}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", mb: 1, fontFamily: G }}>
                      {s.label}
                    </Typography>
                    <Typography sx={{ fontFamily: S, fontSize: "2.2rem", color: s.color, lineHeight: 1, mb: 0.5 }}>{s.value}</Typography>
                    <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G }}>{s.sub}</Typography>
                  </Grid>
                ))}
              </Grid>
              <Box sx={{ mt: 2, pt: 2, borderTop: "1px solid #E8E3DA" }}>
                <Stack direction="row" spacing={1} flexWrap="wrap" gap={0.8}>
                  <Chip size="small" label={`Mode: ${coverage.ingestion_scope?.wcm_context_mode || "full"}`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA", fontSize: "0.68rem" }} />
                  <Chip size="small" label={`Open/Closed: ${Object.keys(coverage.status_coverage?.open_closed_status || {}).length}`} sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA", fontSize: "0.68rem" }} />
                  <Chip size="small" label={`Payment states: ${Object.keys(coverage.status_coverage?.payment_status || {}).length}`} color="success" />
                </Stack>
              </Box>
            </CardContent>
          </Card>
        </>
      )}

      {/* Context Layer — light green tint */}
      {contextLayer?.context_ready && (
        <>
          <SectionHeader label="Celonis Context Layer" meta="Leadership View" />
          <Card sx={{ background: "#F2FAF6 !important", border: "1px solid #B8DFD0 !important" }}>
            <CardContent>
              <Grid container spacing={3}>
                <Grid item xs={12} md={4}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", mb: 1.5, fontFamily: G }}>Performance</Typography>
                  {[
                    ["Golden Path", `${contextLayer.process_map?.golden_path_percentage ?? 0}%`],
                    ["Observed Process Duration", `${contextLayer.cycle_time?.avg_end_to_end_days ?? 0} days`],
                    ["Process Failure Rate", `${contextLayer.cycle_time?.exception_rate_pct ?? 0}%`],
                  ].map(([k, v]) => (
                    <Box key={k} sx={{ display: "flex", justifyContent: "space-between", mb: 1 }}>
                      <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G }}>{k}</Typography>
                      <Typography sx={{ fontSize: "0.78rem", color: "#17140F", fontWeight: 700, fontFamily: G }}>{v}</Typography>
                    </Box>
                  ))}
                </Grid>
                <Grid item xs={12} md={4}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", mb: 1.5, fontFamily: G }}>High-Impact Process Transitions</Typography>
                  {(contextLayer.process_map?.top_transitions || []).slice(0, 3).map((t, i) => (
                    <Box key={i} sx={{ mb: 1.2 }}>
                      <Typography sx={{ fontSize: "0.75rem", color: "#17140F", fontFamily: G }}>{t.from_step} → {t.to_step}</Typography>
                      <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G }}>{t.avg_transition_days}d average</Typography>
                    </Box>
                  ))}
                </Grid>
                <Grid item xs={12} md={4}>
                  <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#1A6B5E", mb: 1.5, fontFamily: G }}>Exception Contexts</Typography>
                  <Stack spacing={0.8}>
                    {(contextLayer.exception_contexts || []).slice(0, 3).map((ex) => (
                      <Box key={ex.exception_category_id} sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G }}>{ex.exception_category_label}</Typography>
                        <Box sx={{ background: "#1A6B5E", px: 1.2, py: 0.2, borderRadius: "99px" }}>
                          <Typography sx={{ fontSize: "0.7rem", color: "#FFFFFF", fontWeight: 700, fontFamily: G }}>{ex.case_count}</Typography>
                        </Box>
                      </Box>
                    ))}
                  </Stack>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </>
      )}

      {/* Process Trajectory (replaces Golden Path + Turnaround Times) */}
      {(context?.golden_path || (context?.activity_durations && Object.keys(context.activity_durations).length > 0)) && (
        <ProcessTrajectoryView
          goldenPath={context?.golden_path}
          goldenPathPercentage={context?.golden_path_percentage}
          activityDurations={context?.activity_durations}
        />
      )}

      {/* Exception Patterns */}
      {context?.exception_patterns?.length > 0 && (
        <>
          <SectionHeader label="Exception Patterns Discovered" meta={`${context.exception_patterns.length} found`} />
          <Grid container spacing={2}>
            {context.exception_patterns.map((ep, i) => (
              <Grid item xs={12} md={6} key={i}>
                <Card sx={{ borderLeft: "3px solid #B03030 !important" }}>
                  <CardContent>
                    <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 1.5 }}>
                      <Typography sx={{ fontWeight: 600, color: "#B03030", fontSize: "0.875rem", fontFamily: G }}>{ep.exception_type}</Typography>
                      <Box sx={{ background: "#FAEAEA", border: "1px solid #E0A0A0", px: 1, py: 0.2, borderRadius: "99px" }}>
                        <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color: "#B03030", fontFamily: G }}>{ep.frequency_percentage}%</Typography>
                      </Box>
                    </Box>
                    <Grid container spacing={1.5}>
                      {[["Trigger", ep.trigger_condition], ["Resolution", ep.typical_resolution], ["Role", ep.resolution_role]].map(([k, v]) => (
                        <Grid item xs={12} key={k}>
                          <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: "#9C9690", fontFamily: G }}>{k}</Typography>
                          <Typography sx={{ fontSize: "0.8rem", color: "#5C5650", fontFamily: G }}>{v}</Typography>
                        </Grid>
                      ))}
                    </Grid>
                    <Box sx={{ mt: 1.5, background: "#DCF0EB", border: "1px solid #8FCFC5", borderRadius: "8px", px: 1.5, py: 0.7, display: "inline-block" }}>
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 600, color: "#1A6B5E", fontFamily: G }}>Avg resolution: {ep.avg_resolution_time_days} days</Typography>
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </>
      )}

      {/* Conformance Violations */}
      {context?.conformance_violations?.length > 0 && (
        <>
          <SectionHeader label="Conformance Violations" meta={`${context.conformance_violations.length} detected`} />
          <Stack spacing={1.5}>
            {context.conformance_violations.map((v, i) => (
              <Card key={i} sx={{ borderLeft: "3px solid #A05A10 !important" }}>
                <CardContent sx={{ py: "14px !important" }}>
                  <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 2 }}>
                    <Box sx={{ flex: 1 }}>
                      <Typography sx={{ fontWeight: 600, color: "#A05A10", fontSize: "0.875rem", mb: 0.4, fontFamily: G }}>{v.rule}</Typography>
                      <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G }}>{v.violation_description}</Typography>
                    </Box>
                    <Box sx={{ textAlign: "right", flexShrink: 0 }}>
                      <Typography sx={{ fontFamily: S, fontSize: "1.7rem", color: "#A05A10", lineHeight: 1 }}>{v.violation_rate}%</Typography>
                      <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G }}>{v.affected_cases} cases</Typography>
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            ))}
          </Stack>
        </>
      )}
    </div>
  );
}
