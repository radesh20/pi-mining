import React, { useEffect, useState } from "react";
import { Card, CardContent, Chip, Grid } from "@mui/material";
import Typography from "@mui/material/Typography";
import AgentCard from "../components/AgentCard";
import ProcessMetrics from "../components/ProcessMetrics";
import LoadingSpinner from "../components/LoadingSpinner";
import { fetchProcessAgents } from "../api/client";

export default function ProcessAgentsView() {
  const [agents, setAgents] = useState(null);
  const [context, setContext] = useState(null);
  const [lifecycleMap, setLifecycleMap] = useState([]);
  const [piVsBiMessage, setPiVsBiMessage] = useState("");
  const [criticalScenario, setCriticalScenario] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchProcessAgents()
      .then((res) => {
        setAgents(res.data?.recommended_agents || []);
        setLifecycleMap(res.data?.lifecycle_map || []);
        setPiVsBiMessage(res.data?.pi_vs_bi_message || "");
        setCriticalScenario(res.data?.critical_timing_scenario || null);
        setContext(res.process_context);
      })
      .catch((err) => setError(err.response?.data?.detail || err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingSpinner message="Analyzing Celonis data and recommending agents via Azure OpenAI..." />;

  if (error) {
    return (
      <div className="page-container">
        <div className="error-box">
          <Typography variant="h6">Error</Typography>
          <Typography variant="body2">{error}</Typography>
        </div>
      </div>
    );
  }

  return (
    <div className="page-container">
      <Typography variant="h4" sx={{ fontWeight: 700, mb: 1, color: "#fff" }}>
        📋 Deliverable 1: Process Agents View
      </Typography>
      <Typography variant="body1" sx={{ color: "#888", mb: 3 }}>
        Agents recommended from Celonis process mining data
      </Typography>

      {piVsBiMessage && (
        <Card sx={{ mb: 2, background: "#f8fafc", border: "1px solid #cbd5e1" }}>
          <CardContent>
            <Typography variant="subtitle2" sx={{ color: "#0f172a", fontWeight: 700, mb: 0.8 }}>
              Why PI, not BI-only
            </Typography>
            <Typography variant="body2" sx={{ color: "#334155" }}>
              {piVsBiMessage}
            </Typography>
          </CardContent>
        </Card>
      )}

      {criticalScenario && (
        <Card sx={{ mb: 2, background: "#ecfeff", border: "1px solid #67e8f9" }}>
          <CardContent>
            <Typography variant="subtitle2" sx={{ color: "#0e7490", fontWeight: 700, mb: 0.8 }}>
              Critical Timing Scenario
            </Typography>
            <Typography variant="body2" sx={{ color: "#0f172a", mb: 0.8 }}>
              {criticalScenario.scenario}
            </Typography>
            <Chip
              size="small"
              label={`Due in ${criticalScenario.days_until_due}d | Historical path ${criticalScenario.historical_processing_days}d`}
              sx={{ mr: 0.8, mb: 0.6, background: "#cffafe", color: "#155e75" }}
            />
            <Chip
              size="small"
              label={criticalScenario.pi_recommendation || "Act now"}
              sx={{ mb: 0.6, background: "#dbeafe", color: "#1d4ed8" }}
            />
            <Typography variant="caption" sx={{ display: "block", color: "#155e75", mt: 0.6 }}>
              {criticalScenario.why_bi_only_misses_this}
            </Typography>
          </CardContent>
        </Card>
      )}

      {context && <ProcessMetrics context={context} />}

      {lifecycleMap.length > 0 && (
        <>
          <Typography className="section-title">P2P Lifecycle Map (Top Process Agents)</Typography>
          <Grid container spacing={1.2} sx={{ mb: 2 }}>
            {lifecycleMap.map((item) => (
              <Grid key={item.lifecycle_stage} item xs={12} md={6}>
                <Card sx={{ height: "100%" }}>
                  <CardContent>
                    <Typography variant="subtitle2" sx={{ fontWeight: 700, color: "#0f172a", mb: 0.8 }}>
                      {item.lifecycle_stage}
                    </Typography>
                    <Typography variant="body2" sx={{ color: "#1e293b", mb: 0.6 }}>
                      {item.agent_name}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block", color: "#166534", mb: 0.5 }}>
                      PI Evidence: {item.pi_evidence}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block", color: "#1d4ed8", mb: 0.5 }}>
                      Timing Decision: {item.timing_decision}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block", color: "#334155", mb: 0.5 }}>
                      Action Impact: {item.action_impact}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block", color: "#7c2d12" }}>
                      Why BI misses this: {item.why_bi_only_misses_this}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </>
      )}

      <Typography className="section-title">Recommended Process Agents</Typography>
      <div className="card-grid">
        {agents && agents.map((agent, idx) => <AgentCard key={idx} agent={agent} />)}
      </div>
    </div>
  );
}
