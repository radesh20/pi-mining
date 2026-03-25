import React, { useEffect, useState } from "react";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Grid from "@mui/material/Grid";
import Chip from "@mui/material/Chip";
import Button from "@mui/material/Button";
import Alert from "@mui/material/Alert";
import ProcessMetrics from "../components/ProcessMetrics";
import LoadingSpinner from "../components/LoadingSpinner";
import {
  fetchCelonisContextLayer,
  fetchContextCoverage,
  fetchProcessInsights,
  refreshCache,
  validateWcmContext,
} from "../api/client";

export default function Dashboard() {
  const [context, setContext] = useState(null);
  const [coverage, setCoverage] = useState(null);
  const [contextLayer, setContextLayer] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState("");
  const [validationMessage, setValidationMessage] = useState("");
  const [error, setError] = useState(null);

  const loadAll = async () => {
    const [insightsRes, coverageRes, layerRes] = await Promise.all([
      fetchProcessInsights(),
      fetchContextCoverage(),
      fetchCelonisContextLayer(),
    ]);
    setContext(insightsRes.data);
    setCoverage(coverageRes.data || null);
    setContextLayer(layerRes.data || insightsRes.data?.celonis_context_layer || null);
  };

  useEffect(() => {
    loadAll()
      .catch((err) => setError(err.response?.data?.detail || err.message))
      .finally(() => setLoading(false));
  }, []);

  const handleRefresh = async () => {
    setRefreshing(true);
    setRefreshMessage("");
    setError(null);
    try {
      await refreshCache();
      await loadAll();
      setRefreshMessage("Cache refreshed and coverage reloaded.");
    } catch (err) {
      setError(err.response?.data?.detail || err.message || "Refresh failed");
    } finally {
      setRefreshing(false);
    }
  };

  const handleValidate = async () => {
    setValidationMessage("");
    setError(null);
    try {
      const res = await validateWcmContext();
      const data = res.data || {};
      if (data.overall_passed) {
        setValidationMessage("WCM validation passed.");
      } else {
        const first = (data.recommendations || [])[0] || "Validation found mapping/coverage gaps.";
        setValidationMessage(`WCM validation found issues: ${first}`);
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message || "Validation failed");
    }
  };

  if (loading) return <LoadingSpinner message="Extracting process insights from Celonis..." />;

  if (error) {
    return (
      <div className="page-container">
        <div className="error-box">
          <Typography variant="h6">Celonis Connection Required</Typography>
          <Typography variant="body2">{error}</Typography>
          <Typography variant="body2" sx={{ mt: 1 }}>
            Go to Celonis Setup page to verify connection.
          </Typography>
        </div>
      </div>
    );
  }

  return (
    <div className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, mb: 1, color: "#fff" }}>
        Process Mining → AI Agents
      </Typography>
      <Typography variant="body1" sx={{ color: "#888", mb: 3 }}>
        All data sourced live from Celonis Process Mining
      </Typography>

      <Box sx={{ mb: 2, display: "flex", alignItems: "center", gap: 1 }}>
        <Button variant="contained" onClick={handleRefresh} disabled={refreshing}>
          {refreshing ? "Refreshing..." : "Refresh Cache + Coverage"}
        </Button>
        <Button variant="outlined" onClick={handleValidate} disabled={refreshing}>
          Validate WCM Context
        </Button>
        {coverage?.refresh?.last_refreshed_at && (
          <Typography variant="caption" sx={{ color: "#94a3b8" }}>
            Last refresh: {coverage.refresh.last_refreshed_at}
          </Typography>
        )}
      </Box>

      {refreshMessage && (
        <Alert severity="success" sx={{ mb: 2 }}>
          {refreshMessage}
        </Alert>
      )}
      {validationMessage && (
        <Alert severity={validationMessage.includes("issues") ? "warning" : "success"} sx={{ mb: 2 }}>
          {validationMessage}
        </Alert>
      )}

      <ProcessMetrics context={context} />

      {coverage && (
        <Card sx={{ mt: 2, border: "1px solid #334155", background: "#0f172a" }}>
          <CardContent>
            <Typography variant="h6" sx={{ color: "#93c5fd", fontWeight: 700, mb: 1 }}>
              WCM Context Coverage
            </Typography>
            <Grid container spacing={1}>
              <Grid item xs={12} md={4}>
                <Typography variant="body2" sx={{ color: "#cbd5e1" }}>
                  Cases: {coverage.coverage?.total_cases ?? 0} | Events: {coverage.coverage?.total_events ?? 0}
                </Typography>
                <Typography variant="body2" sx={{ color: "#cbd5e1" }}>
                  OLAP Rows: {coverage.coverage?.olap_rows ?? 0} | Tables: {coverage.coverage?.tables_extracted ?? 0}
                </Typography>
              </Grid>
              <Grid item xs={12} md={4}>
                <Typography variant="body2" sx={{ color: "#cbd5e1" }}>
                  Exception Categories: {coverage.coverage?.exception_category_count ?? 0}
                </Typography>
                <Typography variant="body2" sx={{ color: "#cbd5e1" }}>
                  Exception Records: {coverage.coverage?.exception_record_count ?? 0}
                </Typography>
              </Grid>
              <Grid item xs={12} md={4}>
                <Typography variant="body2" sx={{ color: "#cbd5e1", mb: 0.5 }}>
                  Mode: {coverage.ingestion_scope?.wcm_context_mode || "full"}
                </Typography>
                <Typography variant="caption" sx={{ display: "block", color: "#94a3b8", mb: 0.4 }}>
                  OLAP table: {coverage.mapping_diagnostics?.olap_source_table || "N/A"} ({coverage.mapping_diagnostics?.olap_selection_mode || "auto"})
                </Typography>
                {coverage.mapping_diagnostics?.has_missing_olap_mappings && (
                  <Typography variant="caption" sx={{ display: "block", color: "#fca5a5", mb: 0.4 }}>
                    Missing required mappings: {(coverage.mapping_diagnostics?.olap_missing_required_fields || []).join(", ")}
                  </Typography>
                )}
                <Chip
                  size="small"
                  label={`Open/Closed states: ${Object.keys(coverage.status_coverage?.open_closed_status || {}).length}`}
                  sx={{ mr: 0.5, mb: 0.5, background: "#1e293b", color: "#93c5fd" }}
                />
                <Chip
                  size="small"
                  label={`Payment states: ${Object.keys(coverage.status_coverage?.payment_status || {}).length}`}
                  sx={{ mb: 0.5, background: "#1e293b", color: "#86efac" }}
                />
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      )}

      {contextLayer?.context_ready && (
        <Card sx={{ mt: 2, border: "1px solid #22c55e", background: "#052e16" }}>
          <CardContent>
            <Typography variant="h6" sx={{ color: "#bbf7d0", fontWeight: 700, mb: 1 }}>
              Celonis Context Layer (Leadership View)
            </Typography>
            <Grid container spacing={1}>
              <Grid item xs={12} md={4}>
                <Typography variant="caption" sx={{ display: "block", color: "#dcfce7" }}>
                  Golden Path Coverage: {contextLayer.process_map?.golden_path_percentage ?? 0}%
                </Typography>
                <Typography variant="caption" sx={{ display: "block", color: "#dcfce7" }}>
                  Avg E2E Cycle Time: {contextLayer.cycle_time?.avg_end_to_end_days ?? 0} days
                </Typography>
                <Typography variant="caption" sx={{ display: "block", color: "#dcfce7" }}>
                  Exception Rate: {contextLayer.cycle_time?.exception_rate_pct ?? 0}%
                </Typography>
              </Grid>
              <Grid item xs={12} md={4}>
                <Typography variant="caption" sx={{ display: "block", color: "#dcfce7", mb: 0.5 }}>
                  Top Process Steps:
                </Typography>
                {(contextLayer.process_map?.top_transitions || []).slice(0, 3).map((t, idx) => (
                  <Typography key={idx} variant="caption" sx={{ display: "block", color: "#bbf7d0" }}>
                    {t.from_step} → {t.to_step} ({t.avg_transition_days}d)
                  </Typography>
                ))}
              </Grid>
              <Grid item xs={12} md={4}>
                <Typography variant="caption" sx={{ display: "block", color: "#dcfce7", mb: 0.5 }}>
                  Exception Contexts:
                </Typography>
                {(contextLayer.exception_contexts || []).slice(0, 3).map((ex) => (
                  <Chip
                    key={ex.exception_category_id}
                    size="small"
                    label={`${ex.exception_category_label}: ${ex.case_count}`}
                    sx={{ mr: 0.5, mb: 0.5, background: "#14532d", color: "#dcfce7" }}
                  />
                ))}
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      )}

      {context?.golden_path && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" sx={{ color: "#6C63FF", fontWeight: 600 }}>
            🏆 Golden Path ({context.golden_path_percentage}% of cases)
          </Typography>
          <Typography variant="body2" sx={{ color: "#ccc", mt: 1 }}>
            {context.golden_path}
          </Typography>
        </Box>
      )}

      {context?.activity_durations && Object.keys(context.activity_durations).length > 0 && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" sx={{ color: "#6C63FF", fontWeight: 600, mb: 1 }}>
            ⏱ Turnaround Times (from Celonis Process Explorer)
          </Typography>
          {Object.entries(context.activity_durations).map(([key, val]) => (
            <Typography key={key} variant="body2" sx={{ color: "#aaa", ml: 1 }}>
              {key}: <strong style={{ color: "#00D4AA" }}>{val} days</strong>
            </Typography>
          ))}
        </Box>
      )}

      {context?.exception_patterns && context.exception_patterns.length > 0 && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" sx={{ color: "#ff5252", fontWeight: 600, mb: 1 }}>
            ⚠️ Exception Patterns Discovered
          </Typography>
          {context.exception_patterns.map((ep, i) => (
            <Box key={i} className="step-card" sx={{ mb: 1, borderLeftColor: "#ff5252" }}>
              <Typography variant="subtitle2" sx={{ color: "#ff5252" }}>
                {ep.exception_type} ({ep.frequency_percentage}%)
              </Typography>
              <Typography variant="body2" sx={{ color: "#aaa" }}>
                Trigger: {ep.trigger_condition}
              </Typography>
              <Typography variant="body2" sx={{ color: "#aaa" }}>
                Resolution: {ep.typical_resolution} by {ep.resolution_role}
              </Typography>
              <Typography variant="caption" sx={{ color: "#00D4AA" }}>
                Avg resolution: {ep.avg_resolution_time_days} days
              </Typography>
            </Box>
          ))}
        </Box>
      )}

      {context?.conformance_violations && context.conformance_violations.length > 0 && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" sx={{ color: "#ffaa00", fontWeight: 600, mb: 1 }}>
            🚨 Conformance Violations
          </Typography>
          {context.conformance_violations.map((v, i) => (
            <Box key={i} className="step-card" sx={{ mb: 1, borderLeftColor: "#ffaa00" }}>
              <Typography variant="subtitle2" sx={{ color: "#ffaa00" }}>
                {v.rule}
              </Typography>
              <Typography variant="body2" sx={{ color: "#aaa" }}>
                {v.violation_description} — {v.violation_rate}% of cases ({v.affected_cases} cases)
              </Typography>
            </Box>
          ))}
        </Box>
      )}
    </div>
  );
}
