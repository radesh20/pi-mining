import React, { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Box, Card, CardContent, Chip, Grid, Stack, Typography } from "@mui/material";
import InteractionFlow from "../components/InteractionFlow";
import LoadingSpinner from "../components/LoadingSpinner";
import { executeInvoiceFlow, fetchAllExceptionRecords, fetchExceptionCategories, waitForCacheReady } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

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

const caseKeyForRecord = (record) => {
  if (!record) return "";
  return [
    record.exception_id || "",
    record.invoice_id || record.document_number || record.case_id || "",
    record.vendor_id || "",
  ].join("::");
};

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

function ConversationCard({ title, color, background, border, children }) {
  return (
    <Box sx={{ p: 1.2, background, border: `1px solid ${border}`, borderRadius: "10px", mb: 1.1 }}>
      <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color, fontFamily: G, mb: 0.5 }}>
        {title}
      </Typography>
      {children}
    </Box>
  );
}

function JsonBlock({ label, value, color = "#475569" }) {
  return (
    <Box sx={{ mt: 0.7 }}>
      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color, fontFamily: G, mb: 0.25 }}>
        {label}
      </Typography>
      <Box
        component="pre"
        sx={{
          m: 0,
          p: 1,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
          fontSize: "0.72rem",
          lineHeight: 1.5,
          color: "#334155",
          background: "#F8FAFC",
          border: "1px solid #E2E8F0",
          borderRadius: "8px",
          fontFamily: "'SFMono-Regular', ui-monospace, monospace",
          maxHeight: "220px",
          overflow: "auto",
        }}
      >
        {typeof value === "string" ? value : JSON.stringify(value || {}, null, 2)}
      </Box>
    </Box>
  );
}

function handoffLabelForStep(step, executionTrace) {
  const handoffs = executionTrace?.handoff_messages || [];
  const direct = handoffs.find((item) => item.from_agent === step.agent);
  return direct?.to_agent || "";
}

function normalizeAgentName(name = "") {
  const text = String(name);
  if (text.includes("Vendor Intelligence")) return "VendorIntelligenceAgent";
  if (text.includes("Exception Agent")) return "ExceptionAgent";
  if (text.includes("Invoice Processing")) return "InvoiceProcessingAgent";
  if (text.includes("Human-in-the-Loop")) return "HumanInLoopAgent";
  if (text.includes("Prompt Writer")) return "PromptWriterAgent";
  if (text.includes("Automation Policy")) return "AutomationPolicyAgent";
  if (text.includes("ERP")) return "ERPPostingLayer";
  return text.replace(/\s+/g, "");
}

function buildPredictedConversation(selectedRecord) {
  const vendorName = selectedRecord?.vendor_name || selectedRecord?.vendor_id || "the selected vendor";
  const invoiceId = selectedRecord?.invoice_id || selectedRecord?.document_number || selectedRecord?.case_id || "the selected case";
  const category = selectedRecord?.category_label || selectedRecord?.exception_type || "exception";
  const risk = String(selectedRecord?.risk_level || "MEDIUM").toUpperCase();
  const finalReceiver = ["CRITICAL", "HIGH"].includes(risk) ? "HumanInLoopAgent" : "ERPPostingLayer";

  return [
    {
      agent: "VendorIntelligenceAgent",
      incoming: { sender: "Orchestrator", receiver: "VendorIntelligenceAgent", intent: `Analyze vendor ${vendorName} for invoice ${invoiceId}.` },
      reasoning: `Build vendor risk context and recurrence evidence for ${category}.`,
      outgoing: { sender: "VendorIntelligenceAgent", receiver: "PromptWriterAgent", intent: "Pass vendor risk and exception portfolio context." },
      promptTrace: null,
    },
    {
      agent: "PromptWriterAgent",
      incoming: { sender: "VendorIntelligenceAgent", receiver: "PromptWriterAgent", intent: "Generate downstream prompts using Celonis evidence." },
      reasoning: "Prepare agent-ready instructions grounded in timing, exception, and financial context.",
      outgoing: { sender: "PromptWriterAgent", receiver: "AutomationPolicyAgent", intent: "Provide process-aware prompt package and guardrails." },
      promptTrace: null,
    },
    {
      agent: "AutomationPolicyAgent",
      incoming: { sender: "PromptWriterAgent", receiver: "AutomationPolicyAgent", intent: "Decide whether to automate, monitor, or escalate." },
      reasoning: `Estimate control posture from ${category} severity and risk ${risk}.`,
      outgoing: { sender: "AutomationPolicyAgent", receiver: "InvoiceProcessingAgent", intent: "Send policy decision and oversight posture." },
      promptTrace: null,
    },
    {
      agent: "InvoiceProcessingAgent",
      incoming: { sender: "AutomationPolicyAgent", receiver: "InvoiceProcessingAgent", intent: "Validate invoice and determine if exception handoff is needed." },
      reasoning: "Check turnaround pressure, invoice quality, and exception candidates before posting.",
      outgoing: { sender: "InvoiceProcessingAgent", receiver: "ExceptionAgent", intent: `Handoff ${category} context with turnaround evidence.` },
      promptTrace: null,
    },
    {
      agent: "ExceptionAgent",
      incoming: { sender: "InvoiceProcessingAgent", receiver: "ExceptionAgent", intent: `Resolve ${category} using process-path evidence.` },
      reasoning: "Choose the next best action and create an action-agent prompt package.",
      outgoing: { sender: "ExceptionAgent", receiver: finalReceiver, intent: ["CRITICAL", "HIGH"].includes(risk) ? "Escalate with high-priority review package." : "Send recommended action to downstream automation layer." },
      promptTrace: null,
    },
  ];
}

function buildLiveConversation({ selectedRecord, executionTrace }) {
  const vendorName = selectedRecord?.vendor_name || selectedRecord?.vendor_id || "the selected vendor";
  const invoiceId = selectedRecord?.invoice_id || selectedRecord?.document_number || selectedRecord?.case_id || "the selected case";
  const steps = executionTrace?.steps || [];
  const handoffs = executionTrace?.handoff_messages || [];
  if (!steps.length) {
    return buildPredictedConversation(selectedRecord).map((step, index) => ({
      ...step,
      reasoning: index === 0
        ? `Preparing live orchestration for invoice ${invoiceId} and vendor ${vendorName}.`
        : step.reasoning,
    }));
  }
  return steps.map((step, idx) => {
    const trace = step.full_output?.prompt_trace || {};
    const incomingHandoff = idx === 0
      ? { from_agent: "Orchestrator", to_agent: step.agent, message_type: "START", payload_summary: `Analyze vendor ${vendorName} and resolve open exceptions for ${invoiceId}.` }
      : handoffs.find((item) => item.to_agent === step.agent) || handoffs[idx - 1] || {};
    const outgoingHandoff = handoffs.find((item) => item.from_agent === step.agent) || {};
    const reasoning = step.full_output?.ai_reasoning
      || step.full_output?.reasoning
      || step.output_summary
      || "Reasoning unavailable for this step.";
    const outgoingIntent = trace.handoff?.execution_prompt
      || trace.handoff?.handoff_intent
      || outgoingHandoff.payload_summary
      || "No downstream handoff recorded.";
    return {
      agent: normalizeAgentName(step.agent),
      promptTrace: trace,
      incoming: {
        sender: incomingHandoff.from_agent || "PreviousAgent",
        receiver: normalizeAgentName(step.agent),
        intent: incomingHandoff.payload_summary || incomingHandoff.message_type || trace.prompt_purpose || step.action,
      },
      reasoning,
      outgoing: {
        sender: normalizeAgentName(step.agent),
        receiver: normalizeAgentName(outgoingHandoff.to_agent || handoffLabelForStep(step, executionTrace) || "Orchestrator"),
        intent: outgoingIntent,
      },
    };
  });
}

export default function CrossAgentInteraction() {
  const [categories, setCategories] = useState([]);
  const [records, setRecords] = useState([]);
  const [selectedRecordId, setSelectedRecordId] = useState("");
  const [invoice, setInvoice] = useState(toInvoicePayload(null));
  const [loadingPage, setLoadingPage] = useState(true);
  const [loadingFlow, setLoadingFlow] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const flowRequestRef = useRef(0);

  const selectedRecord = useMemo(
    () => records.find((record) => record.exception_id === selectedRecordId) || null,
    [records, selectedRecordId],
  );
  const selectedCaseKey = useMemo(() => caseKeyForRecord(selectedRecord), [selectedRecord]);
  const rawExecutionTrace = result?.execution_trace || result?.data?.execution_trace || null;
  const traceCaseKey = useMemo(() => {
    if (!rawExecutionTrace) return "";
    return [
      selectedRecord?.exception_id || "",
      rawExecutionTrace.invoice_id || "",
      rawExecutionTrace.vendor_id || "",
    ].join("::");
  }, [rawExecutionTrace, selectedRecord?.exception_id]);
  const executionTrace = traceCaseKey === selectedCaseKey ? rawExecutionTrace : null;

  useEffect(() => {
    let active = true;
    const load = async (retryIfCacheCold = true) => {
      try {
        const categoriesRes = await fetchExceptionCategories();
        const categoryRows = (categoriesRes.data || categoriesRes || []).filter((row) => Number(row.case_count || 0) > 0);
        if (retryIfCacheCold && categoryRows.length === 0) {
          await waitForCacheReady();
          return await load(false);
        }
        const categoryMap = new Map(categoryRows.map((category) => [category.category_id, category]));
        const allRecordsRes = await fetchAllExceptionRecords();
        const allRecords = (allRecordsRes.data || allRecordsRes || [])
          .filter((row) => row.exception_id)
          .map((row) => {
            const category = categoryMap.get(row.category_id) || null;
            return {
              ...row,
              category_label: row.category_label || category?.category_label || row.exception_type,
            };
          });
        const categoryCounts = new Map();
        const categorySamples = [];
        for (const row of allRecords) {
          const categoryId = row.category_id || row.exception_type || "unknown";
          const currentCount = categoryCounts.get(categoryId) || 0;
          if (currentCount >= 3) continue;
          categoryCounts.set(categoryId, currentCount + 1);
          categorySamples.push(row);
        }
        const seen = new Set();
        const recordRows = categorySamples.filter((row) => {
          const key = `${row.category_id || row.exception_type}::${row.invoice_id || row.document_number || row.case_id || row.exception_id}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
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
    setError("");
  }, [selectedRecord]);

  const runOrchestration = async (payloadOverride = null) => {
    const requestId = ++flowRequestRef.current;
    setLoadingFlow(true);
    setError("");
    if (!payloadOverride) setResult(null);
    try {
      const response = await executeInvoiceFlow({
        ...(payloadOverride || invoice),
        fast_mode: true,
        trace_mode: "interaction_fast",
        ui_mode: "interaction",
      });
      if (flowRequestRef.current !== requestId) return;
      setResult(response);
    } catch (e) {
      if (flowRequestRef.current !== requestId) return;
      setError(e?.response?.data?.detail || e.message || "Execution failed");
    } finally {
      if (flowRequestRef.current === requestId) setLoadingFlow(false);
    }
  };

  useEffect(() => {
    if (!selectedRecord) return;
    runOrchestration(toInvoicePayload(selectedRecord));
  }, [selectedRecord]);

  const totalValueAtRisk = records.reduce((sum, record) => sum + Number(record.invoice_amount || record.value_at_risk || 0), 0);
  const autoCandidates = records.filter((record) => {
    const risk = String(record.risk_level || "").toUpperCase();
    return !["CRITICAL", "HIGH"].includes(risk);
  }).length;
  const exceptionAgentPrompt = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Exception Agent"))?.full_output?.prompt_for_next_agents;
  const automationDecision = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Automation Policy Agent"))?.full_output;
  const humanStep = executionTrace?.steps?.find((step) => String(step.agent || "").includes("Human-in-the-Loop Agent"))?.full_output;
  const conversationSteps = executionTrace?.steps || [];
  const nextBestActionPrompt = executionTrace?.next_best_action_recommender_prompt || null;
  const predictedNextAgent = executionTrace
    ? nextBestActionPrompt?.predicted_next_agent || (
      executionTrace.final_status === "ESCALATED_TO_HUMAN"
      ? "Human-in-the-Loop Agent"
      : executionTrace.final_status === "POSTED"
      ? "ERP / Posting Layer"
      : automationDecision?.recommended_agent || "Exception Agent"
    )
    : "Exception Agent";
  const predictedNextStep = executionTrace
    ? nextBestActionPrompt?.recommended_action || executionTrace.turnaround_assessment?.recommendation || "Continue orchestration using the latest Celonis risk signal."
    : "Run orchestration to generate the next agent handoff.";
  const liveConversation = buildLiveConversation({ selectedRecord, executionTrace });

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
                      <Typography sx={{ fontSize: "0.82rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>{record.category_label || record.exception_type || "Exception"}</Typography>
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
        </Grid>

        <Grid item xs={12} md={7}>
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Prompt Interaction Summary</Typography>
              {selectedRecord ? (
                <>
                  {loadingFlow && (
                    <Alert severity="info" sx={{ mb: 1.5 }}>
                      Building the live orchestration trace. The predicted conversation appears first, then the real prompt exchange replaces it as soon as the agents finish.
                    </Alert>
                  )}
                  <ConversationCard title="Agent Layer (Core Intelligence)" color="#1E4E8C" background="#EBF2FC" border="#90B8E8">
                    <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.35 }}>
                      Orchestrator starts the conversation via MessageBus for: <strong>"Analyze vendor {selectedRecord?.vendor_name || selectedRecord?.vendor_id || "the selected vendor"} and resolve open exceptions."</strong>
                    </Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G }}>
                      Selected case summary: {`${selectedRecord?.category_label || selectedRecord?.exception_type || "Exception"} on invoice ${selectedRecord?.invoice_id || selectedRecord?.document_number || selectedRecord?.case_id || "N/A"} for vendor ${selectedRecord?.vendor_name || selectedRecord?.vendor_id || "N/A"}.`}
                    </Typography>
                    <Typography sx={{ fontSize: "0.72rem", color: "#5C5650", fontFamily: G, mt: 0.45 }}>
                      Live case key: {selectedRecord?.invoice_id || selectedRecord?.document_number || selectedRecord?.case_id || "N/A"} · {selectedRecord?.vendor_name || selectedRecord?.vendor_id || "N/A"} {executionTrace?.started_at ? `· started ${new Date(executionTrace.started_at).toLocaleString()}` : ""}
                    </Typography>
                  </ConversationCard>

                  {liveConversation.map((item, idx) => (
                    <ConversationCard
                      key={`${item.agent}-${idx}`}
                      title={`${idx + 1}. ${item.agent}`}
                      color="#5C5650"
                      background="#FCFBF8"
                      border="#E8E3DA"
                    >
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.25 }}>Incoming AgentMessage</Typography>
                      <Typography sx={{ fontSize: "0.78rem", color: "#17140F", fontFamily: G, mb: 0.5 }}>
                        sender: {item.incoming.sender} · receiver: {item.incoming.receiver} · intent: {item.incoming.intent}
                      </Typography>
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.25 }}>Agent Reasoning</Typography>
                      <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G, mb: 0.5 }}>{item.reasoning}</Typography>
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.25 }}>Outgoing AgentMessage</Typography>
                      <Typography sx={{ fontSize: "0.78rem", color: "#1A6B5E", fontFamily: G }}>
                        sender: {item.outgoing.sender} · receiver: {item.outgoing.receiver} · intent: {item.outgoing.intent}
                      </Typography>

                      {item.promptTrace?.prompt_purpose && (
                        <Box sx={{ mt: 1 }}>
                          <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.25 }}>
                            Prompt Conversation
                          </Typography>
                          <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G, mb: 0.4 }}>
                            Prompt purpose: {item.promptTrace.prompt_purpose}
                          </Typography>
                          {Array.isArray(item.promptTrace.guardrails) && item.promptTrace.guardrails.length > 0 && (
                            <Typography sx={{ fontSize: "0.74rem", color: "#7C5A1F", fontFamily: G, mb: 0.4 }}>
                              Guardrails: {item.promptTrace.guardrails.join(" | ")}
                            </Typography>
                          )}
                          <JsonBlock label="MessageBus Input Taken By Agent" value={item.promptTrace.message_bus_input || {}} color="#1E4E8C" />
                          <JsonBlock label="System Prompt Given To Agent" value={item.promptTrace.system_prompt || "N/A"} color="#7C3AED" />
                          <JsonBlock label="User Prompt Given To Agent" value={item.promptTrace.user_prompt || "N/A"} color="#B45309" />
                          <JsonBlock label="Model Output Returned By Agent" value={item.promptTrace.model_output || {}} color="#1A6B5E" />
                          <JsonBlock label="Handoff Sent To Next Agent" value={item.promptTrace.handoff || {}} color="#B03030" />
                        </Box>
                      )}
                    </ConversationCard>
                  ))}

                  <ConversationCard title="Next Step Prediction" color="#A05A10" background="#FEF3DC" border="#F0C870">
                    <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.35 }}>
                      Predicted next agent: <strong>{predictedNextAgent}</strong>
                    </Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#A05A10", fontFamily: G, mb: 0.35 }}>{predictedNextStep}</Typography>
                    <Typography sx={{ fontSize: "0.74rem", color: "#7C5A1F", fontFamily: G }}>
                      Based on category {selectedRecord?.category_label || selectedRecord?.exception_type || "exception"}, turnaround risk {executionTrace?.turnaround_assessment?.urgency || "MEDIUM"}, and the current recommendation chain from this case's Celonis trace.
                    </Typography>
                  </ConversationCard>

                  {conversationSteps.length > 0 && (
                    <Box sx={{ mt: 1.5 }}>
                      <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.8 }}>
                        Per-Case Agent Conversation
                      </Typography>
                      {conversationSteps.map((step, idx) => {
                        const trace = step.full_output?.prompt_trace || {};
                        const targetAgents = trace.handoff?.target_agents || [];
                        return (
                          <ConversationCard
                            key={`${step.step_number}-${step.agent}`}
                            title={`${idx + 1}. ${step.agent}`}
                            color="#5C5650"
                            background="#FCFBF8"
                            border="#E8E3DA"
                          >
                            <Typography sx={{ fontSize: "0.78rem", color: "#17140F", fontFamily: G, mb: 0.35 }}>
                              Prompt purpose: {trace.prompt_purpose || step.action}
                            </Typography>
                            <Typography sx={{ fontSize: "0.74rem", color: "#1E4E8C", fontFamily: G, mb: 0.3 }}>
                              Input to agent: {Object.keys(trace.message_bus_input || {}).join(", ") || step.input_summary || "N/A"}
                            </Typography>
                            <Typography sx={{ fontSize: "0.74rem", color: "#1A6B5E", fontFamily: G, mb: 0.3 }}>
                              Output from agent: {step.output_summary || "N/A"}
                            </Typography>
                            <Typography sx={{ fontSize: "0.74rem", color: "#7C5A1F", fontFamily: G }}>
                              Sent to next agent: {targetAgents.join(", ") || trace.handoff?.target_agent || handoffLabelForStep(step, executionTrace) || "Final step / no downstream agent"}
                            </Typography>
                          </ConversationCard>
                        );
                      })}
                    </Box>
                  )}
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
                    Auto route / human decision: {automationDecision?.automation_decision || "MONITOR"} · Teams handoff ready: {humanStep ? "Yes" : "Pending"}
                  </Typography>
                </CardContent>
              </Card>

              <Card sx={{ mt: 2 }}>
                <CardContent>
                  <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Exception Resolution + Human Loop</Typography>
                  <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
                    Next best action: {nextBestActionPrompt?.recommended_action || exceptionAgentPrompt?.execution_prompt || executionTrace.turnaround_assessment?.recommendation || "N/A"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G, mb: 0.3 }}>
                    Handoff intent: {nextBestActionPrompt?.handoff_intent || exceptionAgentPrompt?.handoff_intent || "Exception resolution handoff"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#B03030", fontFamily: G, mb: 0.3 }}>
                    Human review package: {humanStep?.case_summary || humanStep?.reason_for_review || "Will be prepared when escalation is required."}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G }}>
                    Teams-ready evidence: {humanStep?.celonis_evidence || executionTrace.steps?.[0]?.celonis_evidence_used || "Celonis evidence attached in the HITL package."}
                  </Typography>
                </CardContent>
              </Card>

              <Card sx={{ mt: 2 }}>
                <CardContent>
                  <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Next Best Action Recommender Prompt</Typography>
                  <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.5 }}>
                    Downstream target agents: {(nextBestActionPrompt?.target_action_agents || []).join(", ") || "N/A"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#1E4E8C", fontFamily: G, mb: 0.4 }}>
                    Reason: {nextBestActionPrompt?.reason || executionTrace.orchestration_reasoning?.recommended_next_action || "N/A"}
                  </Typography>
                  <Typography sx={{ fontSize: "0.76rem", color: "#7C5A1F", fontFamily: G, mb: 0.7 }}>
                    PI rationale: {nextBestActionPrompt?.pi_rationale || "Celonis turnaround and exception context are included for the action agent."}
                  </Typography>
                  <JsonBlock label="Execution Prompt For Action Agent" value={nextBestActionPrompt?.execution_prompt || "N/A"} color="#1A6B5E" />
                  <JsonBlock label="Required Payload Fields" value={nextBestActionPrompt?.required_payload_fields || []} color="#A05A10" />
                  <JsonBlock label="Recommended Payload" value={nextBestActionPrompt?.payload || {}} color="#B03030" />
                </CardContent>
              </Card>
            </>
          )}
        </Grid>
      </Grid>
    </div>
  );
}
