import React, { useEffect, useState } from "react";
import {
  Alert,
  Card,
  CardContent,
  Grid,
  Typography,
} from "@mui/material";
import LoadingSpinner from "../components/LoadingSpinner";
import { fetchAgentDeepDive } from "../api/client";

const AGENT_NAME = "Invoice Exception Agent";

const asList = (value, fallback = []) => {
  if (Array.isArray(value)) return value;
  if (!value) return fallback;
  return [value];
};

export default function AgentDeepDive() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [promptData, setPromptData] = useState(null);

  useEffect(() => {
    fetchAgentDeepDive(AGENT_NAME)
      .then((res) => setPromptData(res.data || res))
      .catch((err) => setError(err.response?.data?.detail || err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner message="Loading deep dive prompts..." />;
  const workflow = asList(promptData?.workflow_prompts?.steps || []);
  const decision = asList(promptData?.decision_logic || []);
  const guardrails = asList(promptData?.guardrails || []);
  const criticalScenario = promptData?.critical_scenario;

  const renderBlock = (item, idx) => (
    <Card key={idx} sx={{ mb: 1, border: "1px solid #e5e7eb", background: "#f8fafc" }}>
      <CardContent>
        <Typography variant="subtitle2" sx={{ color: "#0f172a", fontWeight: 700, mb: 0.4 }}>
          Prompt
        </Typography>
        <Typography variant="body2" sx={{ color: "#1f2937", mb: 0.7 }}>
          {item.prompt || item.instruction || item.rule || item.action || "N/A"}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", color: "#166534", mb: 0.4 }}>
          PI Evidence Used: {item.pi_evidence_used || item.source || "N/A"}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8", mb: 0.4 }}>
          Timing Decision: {item.timing_decision || "N/A"}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", color: "#334155", mb: 0.4 }}>
          Action Impact: {item.action_impact || "N/A"}
        </Typography>
        <Typography variant="caption" sx={{ display: "block", color: "#7c2d12" }}>
          Why BI-only would miss this: {item.why_bi_only_misses_this || "N/A"}
        </Typography>
      </CardContent>
    </Card>
  );

  return (
    <div className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, mb: 0.5, color: "text.primary" }}>
        Agent Deep Dive
      </Typography>
      <Typography variant="body1" sx={{ color: "text.secondary", mb: 2 }}>
        {AGENT_NAME} prompt design for core exception handling workflows.
      </Typography>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      {promptData?.pi_superiority_summary && (
        <Alert severity="info" sx={{ mb: 2 }}>
          {promptData.pi_superiority_summary}
        </Alert>
      )}

      {criticalScenario && (
        <Card sx={{ mb: 2, background: "#ecfeff", border: "1px solid #67e8f9" }}>
          <CardContent>
            <Typography variant="subtitle1" sx={{ color: "#0e7490", fontWeight: 700, mb: 0.6 }}>
              Leadership Scenario
            </Typography>
            <Typography variant="body2" sx={{ color: "#0f172a", mb: 0.4 }}>
              {criticalScenario.scenario}
            </Typography>
            <Typography variant="caption" sx={{ display: "block", color: "#155e75", mb: 0.4 }}>
              Days Until Due: {criticalScenario.days_until_due} | Historical Processing: {criticalScenario.historical_processing_days}
            </Typography>
            <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8", mb: 0.4 }}>
              Recommendation: {criticalScenario.recommended_action}
            </Typography>
            <Typography variant="caption" sx={{ display: "block", color: "#7c2d12" }}>
              {criticalScenario.why_pi_is_better}
            </Typography>
          </CardContent>
        </Card>
      )}

      <Card sx={{ mb: 2 }}>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 1.5, color: "text.primary" }}>
            Prompt Blocks
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} md={4}>
              <Typography variant="subtitle2" sx={{ color: "#1976d2", fontWeight: 700, mb: 0.8 }}>
                Workflow Prompts
              </Typography>
              {workflow.map(renderBlock)}
            </Grid>
            <Grid item xs={12} md={4}>
              <Typography variant="subtitle2" sx={{ color: "#ed6c02", fontWeight: 700, mb: 0.8 }}>
                Decision Logic
              </Typography>
              {decision.map(renderBlock)}
            </Grid>
            <Grid item xs={12} md={4}>
              <Typography variant="subtitle2" sx={{ color: "#d32f2f", fontWeight: 700, mb: 0.8 }}>
                Guardrails
              </Typography>
              {guardrails.map(renderBlock)}
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </div>
  );
}
