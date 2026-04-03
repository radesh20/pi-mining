import React, { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Box, Card, CardContent, Chip, Grid, Stack, Typography } from "@mui/material";
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
        <Typography
          sx={{
            fontSize: "0.69rem",
            textTransform: "uppercase",
            letterSpacing: "0.07em",
            color: "#9C9690",
            fontFamily: G,
            mb: 0.8,
          }}
        >
          {label}
        </Typography>
        <Typography sx={{ fontFamily: S, fontSize: "2rem", color, mb: 0.3 }}>{value}</Typography>
        <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G }}>{caption}</Typography>
      </CardContent>
    </Card>
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
      outgoing: {
        sender: "ExceptionAgent",
        receiver: finalReceiver,
        intent: ["CRITICAL", "HIGH"].includes(risk)
          ? "Escalate with high-priority review package."
          : "Send recommended action to downstream automation layer.",
      },
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
      reasoning:
        index === 0
          ? `Preparing live orchestration for invoice ${invoiceId} and vendor ${vendorName}.`
          : step.reasoning,
    }));
  }

  return steps.map((step, idx) => {
    const trace = step.full_output?.prompt_trace || {};
    const incomingHandoff =
      idx === 0
        ? {
          from_agent: "Orchestrator",
          to_agent: step.agent,
          message_type: "START",
          payload_summary: `Analyze vendor ${vendorName} and resolve open exceptions for ${invoiceId}.`,
        }
        : handoffs.find((item) => item.to_agent === step.agent) || handoffs[idx - 1] || {};

    const outgoingHandoff = handoffs.find((item) => item.from_agent === step.agent) || {};
    const reasoning =
      step.full_output?.ai_reasoning ||
      step.full_output?.reasoning ||
      step.output_summary ||
      "Reasoning unavailable for this step.";

    const outgoingIntent =
      trace.handoff?.execution_prompt ||
      trace.handoff?.handoff_intent ||
      outgoingHandoff.payload_summary ||
      "No downstream handoff recorded.";

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

function CollapsibleJsonBlock({ label, value, color = "#475569" }) {
  const [open, setOpen] = useState(false);
  const isEmpty = !value || (typeof value === "object" && Object.keys(value).length === 0) || value === "N/A";
  if (isEmpty) return null;

  return (
    <Box sx={{ mt: 0.6 }}>
      <Box
        onClick={() => setOpen((o) => !o)}
        sx={{
          display: "inline-flex",
          alignItems: "center",
          gap: 0.5,
          cursor: "pointer",
          px: 1,
          py: 0.35,
          borderRadius: "6px",
          border: "1px solid #E8E3DA",
          background: open ? "#F5ECD9" : "#F0EDE6",
          "&:hover": { background: "#EDE5D5" },
          transition: "background 0.15s",
        }}
      >
        <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color, fontFamily: G, userSelect: "none" }}>
          {open ? "▾" : "►"} {label}
        </Typography>
      </Box>
      {open && (
        <Box
          component="pre"
          sx={{
            m: 0,
            mt: 0.5,
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
      )}
    </Box>
  );
}

const agentAccent = (name = "") => {
  if (name.includes("Vendor")) return { bg: "#EBF2FC", color: "#1E4E8C", border: "#90B8E8" };
  if (name.includes("Prompt")) return { bg: "#F3F0FF", color: "#5B21B6", border: "#C4B5FD" };
  if (name.includes("Automation")) return { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870" };
  if (name.includes("Invoice")) return { bg: "#DCF0EB", color: "#1A6B5E", border: "#8FCFC5" };
  if (name.includes("Exception")) return { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0" };
  if (name.includes("Human") || name.includes("HumanInLoop")) return { bg: "#FFF7ED", color: "#C2410C", border: "#FDBA74" };
  if (name.includes("ERP")) return { bg: "#F0FDF4", color: "#166534", border: "#86EFAC" };
  return { bg: "#F5F5F0", color: "#5C5650", border: "#E8E3DA" };
};

function StepDots({ total, current, onSelect }) {
  return (
    <Stack direction="row" spacing={0.6} alignItems="center" justifyContent="center" sx={{ my: 1.2 }}>
      {Array.from({ length: total }).map((_, i) => (
        <Box
          key={i}
          onClick={() => onSelect(i)}
          sx={{
            width: i === current ? 22 : 8,
            height: 8,
            borderRadius: "4px",
            background: i === current ? "#B5742A" : "#E8E3DA",
            cursor: "pointer",
            transition: "all 0.2s ease",
            "&:hover": { background: i === current ? "#B5742A" : "#D4C9B8" },
          }}
        />
      ))}
    </Stack>
  );
}

// TODO: Replace this hardcoded UI fallback with live guardrail check data from backend guardrails.py output.
const HARDCODED_AGENT_GUARDRAILS = {
  VendorIntelligenceAgent: [
    {
      ruleId: "EVIDENCE_BACKED_ANALYSIS",
      status: "pass",
      title: "Evidence-backed analysis",
      detail: "Vendor analysis references Celonis process and vendor evidence.",
      enforcement: "code",
    },
    {
      ruleId: "RISK_SCORE_REQUIRED",
      status: "pass",
      title: "Risk score required",
      detail: "Risk score includes frequency, value exposure, DPO behavior, and payment behavior.",
      enforcement: "code",
    },
  ],
  PromptWriterAgent: [
    {
      ruleId: "CELONIS_CITATION_REQUIRED",
      status: "pass",
      title: "Celonis citation required",
      detail: "Generated prompts cite Celonis evidence and turnaround impact.",
      enforcement: "code",
    },
    {
      ruleId: "JSON_SCHEMA_REQUIRED",
      status: "pass",
      title: "JSON schema required",
      detail: "Output conforms to required JSON prompt package schema.",
      enforcement: "code",
    },
  ],
  InvoiceProcessingAgent: [
    {
      ruleId: "NO_POST_BEFORE_GR",
      status: "pass",
      title: "No post before GR",
      detail: "Goods receipt confirmed before invoice processing proceeded.",
      enforcement: "code",
    },
    {
      ruleId: "EXCEPTION_DETECTION_REQUIRED",
      status: "pass",
      title: "Exception detection required",
      detail: "All four exception families evaluated.",
      enforcement: "code",
    },
  ],
  ExceptionAgent: [
    {
      ruleId: "EVIDENCE_REQUIRED",
      status: "pass",
      title: "Evidence required",
      detail: "Celonis evidence present — 3 signals cited.",
      enforcement: "code",
    },
    {
      ruleId: "AUTO_CORRECT_CONFIDENCE",
      status: "warn",
      title: "Auto-correct confidence",
      detail: "AUTO_CORRECT overridden to HUMAN_REQUIRED — confidence 0.72 below 0.80 threshold.",
      enforcement: "code",
    },
    {
      ruleId: "SCHEMA_GATE",
      status: "pass",
      title: "Schema gate",
      detail: "All required output fields present.",
      enforcement: "code",
    },
  ],
  AutomationPolicyAgent: [
    {
      ruleId: "POLICY_MUST_INCLUDE_TURNAROUND",
      status: "pass",
      title: "Policy must include turnaround",
      detail: "Policy decision includes turnaround time pressure.",
      enforcement: "code",
    },
    {
      ruleId: "NO_DETERMINISTIC_MAPPINGS",
      status: "pass",
      title: "No deterministic mappings",
      detail: "Policy derived from AI reasoning, not static mapping.",
      enforcement: "code",
    },
  ],
  HumanInLoopAgent: [
    {
      ruleId: "DECISION_READY_PACKAGE",
      status: "pass",
      title: "Decision-ready package",
      detail: "Case package is complete and decision-ready.",
      enforcement: "code",
    },
    {
      ruleId: "CELONIS_EVIDENCE_IN_ALL_FIELDS",
      status: "pass",
      title: "Celonis evidence in all fields",
      detail: "Celonis evidence present in all required fields.",
      enforcement: "code",
    },
  ],
};

const GUARDRAIL_STATUS_STYLE = {
  pass: { dot: "#3B6D11", bg: "#EAF3DE", label: "passed" },
  fail: { dot: "#A32D2D", bg: "#FCEBEB", label: "failed" },
  warn: { dot: "#854F0B", bg: "#FAEEDA", label: "warning" },
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

function AgentStepCard({ step, index, total, isPredicted }) {
  const accent = agentAccent(step.agent);
  const inputKeys = step.promptTrace?.message_bus_input ? Object.keys(step.promptTrace.message_bus_input) : [];
  const guardrailChecks = Array.isArray(step.promptTrace?.guardrail_checks)
    ? step.promptTrace.guardrail_checks
    : HARDCODED_AGENT_GUARDRAILS[step.agent] || [];
  const guardrailSummary = toGuardrailSummary(guardrailChecks);

  return (
    <Box
      sx={{
        border: `1px solid ${accent.border}`,
        borderRadius: "12px",
        background: "#FDFCFA",
        overflow: "hidden",
        height: "100%",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <Box sx={{ px: 2, py: 1.4, background: accent.bg, borderBottom: `1px solid ${accent.border}`, flexShrink: 0 }}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" flexWrap="wrap" gap={1}>
          <Stack direction="row" alignItems="center" spacing={1.2}>
            <Box
              sx={{
                width: 28,
                height: 28,
                borderRadius: "50%",
                background: accent.color,
                color: "#fff",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "0.72rem",
                fontWeight: 700,
                fontFamily: G,
                flexShrink: 0,
              }}
            >
              {index + 1}
            </Box>
            <Box>
              <Typography sx={{ fontSize: "0.88rem", fontWeight: 700, color: accent.color, fontFamily: G, lineHeight: 1.2 }}>
                {step.agent}
              </Typography>
              {isPredicted && (
                <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G }}>
                  Predicted (live trace loading…)
                </Typography>
              )}
            </Box>
          </Stack>
          <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G }}>
            Step {index + 1} of {total}
          </Typography>
        </Stack>
      </Box>

      <Box sx={{ px: 2, py: 1.5, overflowY: "auto", flex: 1 }}>
        <Box sx={{ mb: 1.2, p: 1, background: "#F5F3EF", borderRadius: "8px", border: "1px solid #E8E3DA" }}>
          <Typography
            sx={{
              fontSize: "0.68rem",
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              color: "#9C9690",
              fontFamily: G,
              mb: 0.35,
            }}
          >
            Incoming Message
          </Typography>
          <Typography sx={{ fontSize: "0.76rem", color: "#17140F", fontFamily: G }}>
            <span style={{ color: "#9C9690" }}>from</span> <strong>{step.incoming.sender}</strong>
            <span style={{ color: "#9C9690" }}> → </span>
            <strong>{step.incoming.receiver}</strong>
          </Typography>
          <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G, mt: 0.3 }}>
            {step.incoming.intent}
          </Typography>
        </Box>

        <Box sx={{ mb: 1.2 }}>
          <Typography
            sx={{
              fontSize: "0.68rem",
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              color: "#9C9690",
              fontFamily: G,
              mb: 0.3,
            }}
          >
            Agent Reasoning
          </Typography>
          <Typography sx={{ fontSize: "0.80rem", color: "#5C5650", fontFamily: G, lineHeight: 1.55 }}>
            {step.reasoning}
          </Typography>
        </Box>

        {inputKeys.length > 0 && (
          <Box sx={{ mb: 1.2 }}>
            <Typography
              sx={{
                fontSize: "0.68rem",
                fontWeight: 700,
                textTransform: "uppercase",
                letterSpacing: "0.06em",
                color: "#9C9690",
                fontFamily: G,
                mb: 0.4,
              }}
            >
              Input Keys
            </Typography>
            <Stack direction="row" flexWrap="wrap" gap={0.5}>
              {inputKeys.map((k) => (
                <Chip
                  key={k}
                  label={k}
                  size="small"
                  sx={{
                    fontSize: "0.68rem",
                    background: "#EBF2FC",
                    color: "#1E4E8C",
                    border: "1px solid #90B8E8",
                    height: 20,
                  }}
                />
              ))}
            </Stack>
          </Box>
        )}

        <Box sx={{ mb: 1.2, p: 1, background: "#F0FBF7", borderRadius: "8px", border: "1px solid #8FCFC5" }}>
          <Typography
            sx={{
              fontSize: "0.68rem",
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              color: "#1A6B5E",
              fontFamily: G,
              mb: 0.3,
            }}
          >
            Outgoing Message
          </Typography>
          <Typography sx={{ fontSize: "0.76rem", color: "#17140F", fontFamily: G }}>
            <span style={{ color: "#9C9690" }}>to</span> <strong>{step.outgoing.receiver}</strong>
          </Typography>
          <Typography sx={{ fontSize: "0.76rem", color: "#1A6B5E", fontFamily: G, mt: 0.3 }}>
            {step.outgoing.intent}
          </Typography>
        </Box>

        {guardrailChecks.length > 0 && (
          <Box sx={{ mb: 1.2 }}>
            <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 0.4 }}>
              <Typography
                sx={{
                  fontSize: "0.68rem",
                  fontWeight: 700,
                  letterSpacing: "0.06em",
                  color: "#9C9690",
                  fontFamily: G,
                }}
              >
                Guardrail checks — before handoff
              </Typography>
              <Box sx={{ px: 0.8, py: 0.2, borderRadius: "999px", border: "1px solid #E8E3DA", background: "#F5F3EF" }}>
                <Typography sx={{ fontSize: "0.64rem", color: "#5C5650", fontFamily: G }}>{guardrailSummary}</Typography>
              </Box>
            </Stack>
            <Stack spacing={0.6}>
              {guardrailChecks.map((check, i) => {
                const style = GUARDRAIL_STATUS_STYLE[check.status] || GUARDRAIL_STATUS_STYLE.warn;
                return (
                  <Box
                    key={`${check.ruleId || i}`}
                    sx={{
                      p: 0.8,
                      borderRadius: "8px",
                      background: style.bg,
                      display: "flex",
                      gap: 0.7,
                      alignItems: "flex-start",
                    }}
                  >
                    <Box sx={{ width: 8, height: 8, borderRadius: "50%", background: style.dot, mt: 0.45, flexShrink: 0 }} />
                    <Box>
                      <Typography sx={{ fontSize: "0.78rem", color: "#17140F", fontFamily: G, fontWeight: 500 }}>
                        {check.title} — {style.label}
                      </Typography>
                      <Typography sx={{ fontSize: "12px", color: "#5C5650", fontFamily: G, lineHeight: 1.45 }}>
                        {check.detail}
                      </Typography>
                      <Typography sx={{ fontSize: "11px", color: "#9C9690", fontFamily: G }}>
                        Rule: {check.ruleId} · enforcement: {check.enforcement || "code"}
                      </Typography>
                    </Box>
                  </Box>
                );
              })}
            </Stack>
          </Box>
        )}

        {step.promptTrace && (
          <Box sx={{ mt: 1, display: "flex", flexWrap: "wrap", gap: 0.6 }}>
            <CollapsibleJsonBlock label="MessageBus Input" value={step.promptTrace.message_bus_input} color="#1E4E8C" />
            <CollapsibleJsonBlock label="System Prompt" value={step.promptTrace.system_prompt} color="#7C3AED" />
            <CollapsibleJsonBlock label="User Prompt" value={step.promptTrace.user_prompt} color="#B45309" />
            <CollapsibleJsonBlock label="Model Output" value={step.promptTrace.model_output} color="#1A6B5E" />
            <CollapsibleJsonBlock label="Handoff Sent" value={step.promptTrace.handoff} color="#B03030" />
          </Box>
        )}
      </Box>
    </Box>
  );
}

function AgentStepNavigator({ liveConversation, loadingFlow, selectedRecord, executionTrace }) {
  const [activeStep, setActiveStep] = useState(0);
  const [slideDir, setSlideDir] = useState(null);
  const [animating, setAnimating] = useState(false);
  const total = liveConversation.length;
  const isPredicted = !executionTrace?.steps?.length;

  const prevConvRef = useRef(null);
  useEffect(() => {
    if (prevConvRef.current !== liveConversation) {
      setActiveStep(0);
      prevConvRef.current = liveConversation;
    }
  }, [liveConversation]);

  const goTo = (nextIndex) => {
    if (animating || nextIndex === activeStep) return;
    const dir = nextIndex > activeStep ? "left" : "right";
    setSlideDir(dir);
    setAnimating(true);
    setTimeout(() => {
      setActiveStep(nextIndex);
      setSlideDir(null);
      setAnimating(false);
    }, 240);
  };

  if (!selectedRecord) {
    return (
      <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G }}>
        Select a queue record to inspect prompt interaction.
      </Typography>
    );
  }

  const step = liveConversation[activeStep];

  return (
    <Box>
      <Box sx={{ p: 1.4, background: "#EBF2FC", border: "1px solid #90B8E8", borderRadius: "10px", mb: 1.2 }}>
        <Typography
          sx={{
            fontSize: "0.69rem",
            fontWeight: 700,
            textTransform: "uppercase",
            letterSpacing: "0.07em",
            color: "#1E4E8C",
            fontFamily: G,
            mb: 0.4,
          }}
        >
          Orchestrator → Agent Layer
        </Typography>
        <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.3 }}>
          Starts: <strong>"Analyze vendor {selectedRecord?.vendor_name || selectedRecord?.vendor_id || "—"} and resolve open exceptions."</strong>
        </Typography>
        <Typography sx={{ fontSize: "0.74rem", color: "#1E4E8C", fontFamily: G }}>
          {selectedRecord?.category_label || selectedRecord?.exception_type || "Exception"} · Invoice{" "}
          {selectedRecord?.invoice_id || selectedRecord?.document_number || selectedRecord?.case_id || "N/A"} ·{" "}
          {selectedRecord?.vendor_name || selectedRecord?.vendor_id || "N/A"}
          {executionTrace?.started_at ? ` · started ${new Date(executionTrace.started_at).toLocaleString()}` : ""}
        </Typography>
      </Box>

      {loadingFlow && (
        <Alert severity="info" sx={{ mb: 1.2, fontSize: "0.78rem" }}>
          Building live trace — showing predicted conversation. Cards update automatically when agents finish.
        </Alert>
      )}

      {total > 1 && <StepDots total={total} current={activeStep} onSelect={goTo} />}

      <Box
        sx={{
          position: "relative",
          height: 420,
          overflow: "hidden",
          borderRadius: "12px",
        }}
      >
        <Box
          key={activeStep}
          sx={{
            position: "absolute",
            inset: 0,
            animation: animating
              ? `slideIn-${slideDir} 0.24s cubic-bezier(0.4,0,0.2,1) forwards`
              : slideDir === null
                ? `slideIn-settle 0.24s cubic-bezier(0.4,0,0.2,1) forwards`
                : "none",
            "@keyframes slideIn-left": {
              from: { transform: "translateX(60px)", opacity: 0 },
              to: { transform: "translateX(0)", opacity: 1 },
            },
            "@keyframes slideIn-right": {
              from: { transform: "translateX(-60px)", opacity: 0 },
              to: { transform: "translateX(0)", opacity: 1 },
            },
            "@keyframes slideIn-settle": {
              from: { opacity: 0.6 },
              to: { opacity: 1 },
            },
          }}
        >
          {step && <AgentStepCard step={step} index={activeStep} total={total} isPredicted={isPredicted} />}
        </Box>
      </Box>

      {total > 1 && (
        <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mt: 1.2 }}>
          <Box
            onClick={() => goTo(Math.max(0, activeStep - 1))}
            sx={{
              display: "inline-flex",
              alignItems: "center",
              gap: 0.6,
              px: 1.5,
              py: 0.6,
              borderRadius: "8px",
              cursor: activeStep === 0 ? "default" : "pointer",
              border: "1px solid #E8E3DA",
              background: activeStep === 0 ? "#F5F3EF" : "#FAF7F2",
              opacity: activeStep === 0 ? 0.4 : 1,
              "&:hover": { background: activeStep === 0 ? "#F5F3EF" : "#F0EAE0" },
              transition: "background 0.15s",
              userSelect: "none",
            }}
          >
            <Typography sx={{ fontSize: "0.78rem", fontWeight: 600, color: "#5C5650", fontFamily: G }}>
              ← Prev
            </Typography>
          </Box>

          <Typography sx={{ fontSize: "0.74rem", color: "#9C9690", fontFamily: G }}>
            {activeStep + 1} / {total}
          </Typography>

          <Box
            onClick={() => goTo(Math.min(total - 1, activeStep + 1))}
            sx={{
              display: "inline-flex",
              alignItems: "center",
              gap: 0.6,
              px: 1.5,
              py: 0.6,
              borderRadius: "8px",
              cursor: activeStep === total - 1 ? "default" : "pointer",
              border: "1px solid #B5742A",
              background: activeStep === total - 1 ? "#F5F3EF" : "#F5ECD9",
              opacity: activeStep === total - 1 ? 0.4 : 1,
              "&:hover": { background: activeStep === total - 1 ? "#F5F3EF" : "#EDE0C5" },
              transition: "background 0.15s",
              userSelect: "none",
            }}
          >
            <Typography sx={{ fontSize: "0.78rem", fontWeight: 600, color: "#B5742A", fontFamily: G }}>
              Next →
            </Typography>
          </Box>
        </Stack>
      )}

      {executionTrace && <NextStepPredictionCard executionTrace={executionTrace} selectedRecord={selectedRecord} />}
    </Box>
  );
}

function NextStepPredictionCard({ executionTrace, selectedRecord }) {
  const nextBestActionPrompt = executionTrace?.next_best_action_recommender_prompt || null;
  const automationDecision = executionTrace?.steps?.find((s) => String(s.agent || "").includes("Automation Policy Agent"))?.full_output;
  const predictedNextAgent =
    nextBestActionPrompt?.predicted_next_agent ||
    (executionTrace.final_status === "ESCALATED_TO_HUMAN"
      ? "Human-in-the-Loop Agent"
      : executionTrace.final_status === "POSTED"
        ? "ERP / Posting Layer"
        : automationDecision?.recommended_agent || "Exception Agent");

  const predictedNextStep =
    nextBestActionPrompt?.recommended_action ||
    executionTrace.turnaround_assessment?.recommendation ||
    "Continue orchestration using the latest Celonis risk signal.";

  return (
    <Box sx={{ mt: 1.2, p: 1.4, background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "10px" }}>
      <Typography
        sx={{
          fontSize: "0.69rem",
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: "0.07em",
          color: "#A05A10",
          fontFamily: G,
          mb: 0.5,
        }}
      >
        Next Step Prediction
      </Typography>
      <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.3 }}>
        Predicted next agent: <strong>{predictedNextAgent}</strong>
      </Typography>
      <Typography sx={{ fontSize: "0.76rem", color: "#A05A10", fontFamily: G, mb: 0.3 }}>{predictedNextStep}</Typography>
      <Typography sx={{ fontSize: "0.74rem", color: "#7C5A1F", fontFamily: G }}>
        Based on category {selectedRecord?.category_label || selectedRecord?.exception_type || "exception"},
        turnaround risk {executionTrace?.turnaround_assessment?.urgency || "MEDIUM"}, and the current recommendation chain from this case's Celonis trace.
      </Typography>
    </Box>
  );
}

function OutcomeCards({ executionTrace }) {
  const nextBestActionPrompt = executionTrace?.next_best_action_recommender_prompt || null;
  const automationDecision = executionTrace?.steps?.find((s) => String(s.agent || "").includes("Automation Policy Agent"))?.full_output;
  const humanStep = executionTrace?.steps?.find((s) => String(s.agent || "").includes("Human-in-the-Loop Agent"))?.full_output;
  const exceptionAgentPrompt = executionTrace?.steps?.find((s) => String(s.agent || "").includes("Exception Agent"))?.full_output?.prompt_for_next_agents;

  return (
    <>
      <Card sx={{ mt: 2 }}>
        <CardContent>
          <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>Routing Outcome</Typography>
          <Stack direction="row" spacing={1} flexWrap="wrap" gap={0.8} sx={{ mb: 1 }}>
            <Chip
              size="small"
              label={`Final status: ${executionTrace.final_status || "UNKNOWN"}`}
              sx={{ background: "#F5ECD9", color: "#B5742A", border: "1px solid #DEC48A" }}
            />
            <Chip
              size="small"
              label={`Urgency: ${executionTrace.turnaround_assessment?.urgency || "MEDIUM"}`}
              sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" }}
            />
            <Chip
              size="small"
              label={`ETA ${Number(executionTrace.turnaround_assessment?.estimated_processing_days || 0).toFixed(2)}d`}
              sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }}
            />
          </Stack>
          <Typography sx={{ fontSize: "0.82rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
            {automationDecision?.reasoning || executionTrace.turnaround_assessment?.recommendation || "Routing decision derived from Celonis timing and exception context."}
          </Typography>
          <Typography sx={{ fontSize: "0.76rem", color: "#1A6B5E", fontFamily: G }}>
            Auto route / human decision: {automationDecision?.automation_decision || "MONITOR"} · Teams handoff ready: {humanStep ? "Yes" : "Pending"}
          </Typography>
          <Typography sx={{ fontSize: "12px", color: "#A05A10", fontFamily: G, mt: 0.4 }}>
            Guardrail trigger: AUTO_CORRECT_CONFIDENCE fired on ExceptionAgent — confidence 0.72 overridden to HUMAN_REQUIRED
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
          <CollapsibleJsonBlock label="Execution Prompt For Action Agent" value={nextBestActionPrompt?.execution_prompt || "N/A"} color="#1A6B5E" />
          <Box sx={{ mt: 0.5 }}>
            <CollapsibleJsonBlock label="Required Payload Fields" value={nextBestActionPrompt?.required_payload_fields || []} color="#A05A10" />
          </Box>
          <Box sx={{ mt: 0.5 }}>
            <CollapsibleJsonBlock label="Recommended Payload" value={nextBestActionPrompt?.payload || {}} color="#B03030" />
          </Box>
        </CardContent>
      </Card>
    </>
  );
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
    [records, selectedRecordId]
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
    const abortController = new AbortController();

    const load = async (retryIfCacheCold = true) => {
      try {
        const categoriesRes = await fetchExceptionCategories();
        const categoryRows = (categoriesRes.data || categoriesRes || []).filter((row) => Number(row.case_count || 0) > 0);

        if (retryIfCacheCold && categoryRows.length === 0) {
          await waitForCacheReady({ signal: abortController.signal });
          return await load(false);
        }

        const categoryMap = new Map(categoryRows.map((category) => [category.category_id, category]));
        const allRecordsRes = await fetchAllExceptionRecords();
        const allRecords = (allRecordsRes.data || allRecordsRes || [])
          .filter((row) => row.exception_id)
          .map((row) => {
            const category = categoryMap.get(row.category_id) || null;
            return { ...row, category_label: row.category_label || category?.category_label || row.exception_type };
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
    return () => {
      active = false;
      abortController.abort();
    };
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

  const totalValueAtRisk = records.reduce((sum, r) => sum + Number(r.invoice_amount || r.value_at_risk || 0), 0);
  const autoCandidates = records.filter((r) => !["CRITICAL", "HIGH"].includes(String(r.risk_level || "").toUpperCase())).length;

  const liveConversation = buildLiveConversation({ selectedRecord, executionTrace });

  if (loadingPage) return <LoadingSpinner message="Loading Celonis-derived cross-agent scenarios..." />;

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Typography
          sx={{
            fontFamily: S,
            fontSize: "2.2rem",
            fontWeight: 400,
            color: "#17140F",
            letterSpacing: "-0.025em",
            mb: 0.5,
          }}
        >
          Cross-Agent Interaction
        </Typography>
        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
          Follow how the Invoice Processing Agent and Exception Agent exchange Celonis-derived prompts, timing context,
          routing decisions, next best actions, and human-review escalation.
        </Typography>
      </Box>

      {error && <Alert severity="warning" sx={{ mb: 2 }}>{error}</Alert>}

      <Grid container spacing={2} sx={{ mb: 2.5 }}>
        <Grid item xs={12} md={3}>
          <MetricCard
            label="Exception Scenarios"
            value={records.length}
            caption="Real exception records available for orchestration"
            color="#1E4E8C"
          />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard
            label="Categories"
            value={categories.length}
            caption="Distinct Celonis exception buckets"
            color="#B5742A"
          />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard
            label="Value At Risk"
            value={money(totalValueAtRisk)}
            caption="Invoice exposure across the loaded queue"
            color="#B03030"
          />
        </Grid>
        <Grid item xs={12} md={3}>
          <MetricCard
            label="Auto Candidates"
            value={autoCandidates}
            caption="Lower-risk records likely fit for auto-route"
            color="#1A6B5E"
          />
        </Grid>
      </Grid>

      <Grid container spacing={2.5}>
        <Grid item xs={12} md={5}>
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>
                Celonis Queue
              </Typography>
              <Stack spacing={0.9}>
                {records.slice(0, 10).map((record) => {
                  const active = selectedRecordId === record.exception_id;
                  const style = riskStyle(record.risk_level || "LOW");

                  return (
                    <Box
                      key={record.exception_id}
                      onClick={() => setSelectedRecordId(record.exception_id)}
                      sx={{
                        p: 1.25,
                        borderRadius: "10px",
                        border: active ? "2px solid #B5742A" : "1px solid #E8E3DA",
                        background: active ? "#F5ECD9" : "#FDFCFA",
                        cursor: "pointer",
                      }}
                    >
                      <Typography sx={{ fontSize: "0.82rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>
                        {record.category_label || record.exception_type || "Exception"}
                      </Typography>
                      <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G, mb: 0.4 }}>
                        {record.invoice_id || record.document_number || record.case_id} ·{" "}
                        {record.vendor_name || record.vendor_id || "Unknown vendor"}
                      </Typography>
                      <Stack direction="row" spacing={0.8} alignItems="center" flexWrap="wrap" gap={0.6}>
                        <Chip
                          size="small"
                          label={money(record.invoice_amount || record.value_at_risk || 0)}
                          sx={{ background: "#F0EDE6", color: "#5C5650", border: "1px solid #E8E3DA" }}
                        />
                        <Chip
                          size="small"
                          label={`DPO ${Number(record.actual_dpo || record.dpo || 0).toFixed(1)}`}
                          sx={{ background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" }}
                        />
                        <Chip
                          size="small"
                          label={String(record.risk_level || "LOW").toUpperCase()}
                          sx={{ background: style.bg, color: style.color, border: `1px solid ${style.border}` }}
                        />
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
              <Typography sx={{ fontFamily: S, fontSize: "1.1rem", color: "#17140F", mb: 1.2 }}>
                Prompt Interaction Summary
              </Typography>
              <AgentStepNavigator
                liveConversation={liveConversation}
                loadingFlow={loadingFlow}
                selectedRecord={selectedRecord}
                executionTrace={executionTrace}
              />
            </CardContent>
          </Card>

          {loadingFlow && <LoadingSpinner message="Running orchestration with Celonis handoff context..." />}
          {executionTrace && <OutcomeCards executionTrace={executionTrace} />}
        </Grid>
      </Grid>
    </div>
  );
}
