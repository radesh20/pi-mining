import React, { useEffect, useMemo, useRef, useState } from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Alert from "@mui/material/Alert";
import TextField from "@mui/material/TextField";
import Grid from "@mui/material/Grid";
import CircularProgress from "@mui/material/CircularProgress";
import ReactMarkdown from "react-markdown";
import { sendSqlChatMessage } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";


function SqlIcon({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="none">
      <path d="M5 5.5C5 3.84 8.13 2.5 11.99 2.5C15.87 2.5 19 3.84 19 5.5V16.5C19 18.16 15.87 19.5 11.99 19.5C8.13 19.5 5 18.16 5 16.5V5.5Z" fill="#B5742A" fillOpacity="0.12" stroke="#B5742A" strokeWidth="1.4" />
      <path d="M5 10.5C5 12.16 8.13 13.5 11.99 13.5C15.87 13.5 19 12.16 19 10.5" stroke="#B5742A" strokeWidth="1.4" />
      <path d="M5 5.5C5 7.16 8.13 8.5 11.99 8.5C15.87 8.5 19 7.16 19 5.5" stroke="#B5742A" strokeWidth="1.4" />
    </svg>
  );
}

function formatTime(d) {
  return new Date(d).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function SectionHeader({ label, meta, count }) {
  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 1.5 }}>
      <Typography sx={{ fontFamily: S, fontSize: "1rem", fontWeight: 400, color: "#17140F", whiteSpace: "nowrap" }}>
        {label}
      </Typography>
      {meta && (
        <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "99px", px: 1.2, py: 0.2 }}>
          <Typography sx={{ fontSize: "0.68rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{meta}</Typography>
        </Box>
      )}
      {count !== undefined && (
        <Box sx={{ background: "#17140F", borderRadius: "99px", px: 1, py: 0.15 }}>
          <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, color: "#F7F5F0", fontFamily: G }}>{count}</Typography>
        </Box>
      )}
      <Box sx={{ flex: 1, height: "1px", background: "#E8E3DA" }} />
    </Box>
  );
}

function UserMessage({ msg }) {
  return (
    <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 1.5, alignItems: "flex-start" }}>
      <Box sx={{ maxWidth: "70%" }}>
        <Box sx={{ background: "#17140F", color: "#F7F5F0", borderRadius: "14px 14px 4px 14px", px: 2, py: 1.2, fontSize: "0.875rem", fontFamily: G, lineHeight: 1.6, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
          {msg.content}
        </Box>
        <Typography sx={{ fontSize: "0.67rem", color: "#9C9690", fontFamily: G, mt: 0.4, textAlign: "right" }}>
          {formatTime(msg.ts)}
        </Typography>
      </Box>
      <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "#EDE9E1", border: "1px solid #D8D2C8", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
        <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#5C5650", fontFamily: G }}>ME</Typography>
      </Box>
    </Box>
  );
}

function AssistantMessage({ msg }) {
  return (
    <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
      <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
        <SqlIcon size={14} />
      </Box>
      <Box sx={{ maxWidth: "80%", flex: 1 }}>
        {msg.scopeLabel && (
          <Box sx={{ mb: 0.6 }}>
            <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1, py: 0.15, display: "inline-flex", alignItems: "center", gap: 0.5 }}>
              <Typography sx={{ fontSize: "0.62rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{msg.scopeLabel}</Typography>
            </Box>
          </Box>
        )}
        <Box sx={{
          background: msg.isError ? "#FAEAEA" : "#FFFFFF",
          border: `1px solid ${msg.isError ? "#E0A0A0" : "#E8E3DA"}`,
          borderRadius: "4px 14px 14px 14px",
          px: 2, py: 1.4,
          fontSize: "0.85rem", fontFamily: G, lineHeight: 1.65,
          color: msg.isError ? "#B03030" : "#17140F",
          wordBreak: "break-word",
          boxShadow: "0 1px 3px rgba(23,20,15,0.05)",
          "& p": { margin: 0, marginBottom: "5px", "&:last-child": { marginBottom: 0 } },
          "& strong": { fontWeight: 700, color: "#17140F" },
          "& ul, & ol": { paddingLeft: "14px", margin: "3px 0" },
          "& li": { marginBottom: "2px", lineHeight: 1.55, fontSize: "0.84rem" },
          "& code": { fontFamily: "monospace", fontSize: "0.78rem", background: "#F0EDE6", borderRadius: "3px", padding: "1px 4px" },
          "& pre": { background: "#F7F5F0", border: "1px solid #E8E3DA", borderRadius: "8px", padding: "10px", overflowX: "auto" },
        }}>
          {msg.isError ? msg.content : <ReactMarkdown>{msg.content}</ReactMarkdown>}
        </Box>
        <Typography sx={{ fontSize: "0.67rem", color: "#9C9690", fontFamily: G, mt: 0.4 }}>
          {formatTime(msg.ts)}
        </Typography>
      </Box>
    </Box>
  );
}

function TypingIndicator() {
  return (
    <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
      <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
        <SqlIcon size={14} />
      </Box>
      <Box sx={{ background: "#FFFFFF", border: "1px solid #E8E3DA", borderRadius: "4px 14px 14px 14px", px: 2, py: 1.2, display: "flex", gap: "5px", alignItems: "center", boxShadow: "0 1px 3px rgba(23,20,15,0.05)" }}>
        {[0, 0.18, 0.36].map((d, i) => (
          <Box key={i} sx={{ width: 5, height: 5, borderRadius: "50%", background: "#B5742A", animation: "chatBounce 1.2s ease-in-out infinite", animationDelay: `${d}s` }} />
        ))}
      </Box>
      <style>{`@keyframes chatBounce{0%,80%,100%{transform:translateY(0);opacity:.4}40%{transform:translateY(-5px);opacity:1}}`}</style>
    </Box>
  );
}

function AgentCard({ agentName }) {
  if (!agentName) return null;
  return (
    <Card sx={{ mb: 1.5, background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.3)" }}>
      <CardContent sx={{ pb: "12px !important" }}>
        <SectionHeader label="Agent used" />
        <Typography sx={{ fontSize: "0.88rem", fontWeight: 700, color: "#17140F", fontFamily: G }}>
          {agentName}
        </Typography>
        <Typography sx={{ fontSize: "0.7rem", color: "#5C5650", fontFamily: G, mt: 0.4 }}>
          Searches the knowledge database and returns concise scoped answers.
        </Typography>
      </CardContent>
    </Card>
  );
}

function DataUsedCard({ dataSources }) {
  if (!dataSources.length) return null;
  return (
    <Card sx={{ mb: 1.5 }}>
      <CardContent sx={{ pb: "12px !important" }}>
        <SectionHeader label="Data used" count={dataSources.length} />
        <Stack spacing={0.7}>
          {dataSources.map((src) => (
            <Box key={src} sx={{ background: "#F7F5F0", border: "1px solid #E8E3DA", borderRadius: "8px", px: 1.2, py: 0.8 }}>
              <Typography sx={{ fontSize: "0.7rem", color: "#5C5650", fontFamily: G }}>{src}</Typography>
            </Box>
          ))}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default function SQLChat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [caseId, setCaseId] = useState("");
  const [vendorId, setVendorId] = useState("");
  const [agentUsed, setAgentUsed] = useState("");
  const [dataSources, setDataSources] = useState([]);
  const [suggestedQs, setSuggestedQs] = useState([]);
  const [recommendedAction, setRecommendedAction] = useState("");
  const [error, setError] = useState(null);
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isTyping]);

  const hasReplied = useMemo(
    () => messages.some((m) => m.role === "assistant" && !m.isError),
    [messages]
  );
  const showRight = dataSources.length > 0 || hasReplied;
  const scopeActive = !!(caseId || vendorId);

  async function handleSend(text) {
    const msg = (text || input).trim();
    if (!msg || isTyping) return;

    setInput("");
    setError(null);
    setSuggestedQs([]);
    setDataSources([]);

    setMessages((prev) => [...prev, { id: Date.now(), role: "user", content: msg, ts: new Date() }]);
    setIsTyping(true);

    try {
      const history = messages.slice(-6).map((m) => ({ role: m.role, content: m.content }));
      const result = await sendSqlChatMessage({
        message: msg,
        caseId,
        vendorId,
        conversationHistory: history,
      });

      setMessages((prev) => [...prev, {
        id: Date.now() + 1,
        role: "assistant",
        content: result.reply || "No response received.",
        ts: new Date(),
        isError: !result.success,
        scopeLabel: result.scope_label || "",
      }]);

      if (result.agent_used) setAgentUsed(result.agent_used);
      if (result.data_sources) setDataSources(result.data_sources);
      if (result.suggested_questions) setSuggestedQs(result.suggested_questions);
      if (result.next_steps?.[0]) setRecommendedAction(result.next_steps[0]);
    } catch (err) {
      setMessages((prev) => [...prev, {
        id: Date.now() + 1,
        role: "assistant",
        content: `Unable to reach the SQL backend: ${err.message}`,
        ts: new Date(),
        isError: true,
      }]);
      setError(err.message);
    } finally {
      setIsTyping(false);
    }
  }

  function handleKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }

  return (
    <div className="page-container">
      <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
        <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
          <Box>
            <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.5 }}>
              SQL Chatbot
            </Typography>
            <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
              Ask questions on the uploaded knowledge database with optional Case/Vendor scope
            </Typography>
          </Box>
          {messages.length > 0 && (
            <Button variant="outlined" size="small" onClick={() => {
              setMessages([]);
              setAgentUsed("");
              setDataSources([]);
              setSuggestedQs([]);
            }}>
              Clear conversation
            </Button>
          )}
        </Box>
      </Box>

      <Grid container spacing={3} sx={{ height: "calc(100vh - 220px)", minHeight: 500 }}>
        <Grid item xs={12} md={3} sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Card>
            <CardContent>
              <SectionHeader label="Scope" />
              <Stack spacing={1.5}>
                <TextField
                  label="Case ID (optional)"
                  size="small"
                  fullWidth
                  placeholder="e.g. 7000026156"
                  value={caseId}
                  onChange={(e) => setCaseId(e.target.value)}
                />
                <TextField
                  label="Vendor ID (optional)"
                  size="small"
                  fullWidth
                  placeholder="e.g. V411327955"
                  value={vendorId}
                  onChange={(e) => setVendorId(e.target.value)}
                />
                <Box sx={{ background: scopeActive ? "#F2FAF6" : "#F7F5F0", border: scopeActive ? "1px solid #B8DFD0" : "1px solid #E8E3DA", borderRadius: "8px", px: 1.2, py: 0.8 }}>
                  <Typography sx={{ fontSize: "0.72rem", color: scopeActive ? "#1A6B5E" : "#9C9690", fontFamily: G }}>
                    {scopeActive
                      ? `Scoped: ${[caseId && `Case ${caseId}`, vendorId && `Vendor ${vendorId}`].filter(Boolean).join(" + ")}`
                      : "No scope - full knowledge database"}
                  </Typography>
                </Box>
              </Stack>
            </CardContent>
          </Card>

        </Grid>

        <Grid item xs={12} md={5} sx={{ display: "flex", flexDirection: "column" }}>
          <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
            <Box sx={{ flex: 1, overflow: "auto", px: 2.5, py: 2.5, display: "flex", flexDirection: "column", gap: 2 }}>
              {messages.length === 0 ? (
                <Box sx={{ margin: "auto", textAlign: "center", maxWidth: 380, py: 4 }}>
                  <Box sx={{ width: 52, height: 52, borderRadius: "14px", background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.25)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}>
                    <SqlIcon size={24} />
                  </Box>
                  <Typography sx={{ fontFamily: S, fontSize: "1.4rem", color: "#17140F", mb: 1 }}>
                    Ask the knowledge database
                  </Typography>
                  <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, lineHeight: 1.65, mb: 2 }}>
                    Answers are generated from uploaded SAP tables and verified relationships.
                  </Typography>
                </Box>
              ) : (
                messages.map((msg) => (
                  msg.role === "user"
                    ? <UserMessage key={msg.id} msg={msg} />
                    : <AssistantMessage key={msg.id} msg={msg} />
                ))
              )}
              {isTyping && <TypingIndicator />}
              <div ref={messagesEndRef} />
            </Box>

              <Box sx={{ borderTop: "1px solid #E8E3DA", p: 2 }}>
                <Box sx={{ display: "grid", gridTemplateColumns: { xs: "1fr", md: "1.2fr 0.8fr" }, gap: 1.5, mb: 1.5 }}>
                  <Box sx={{ border: "1px solid #E8E3DA", borderRadius: "10px", background: "#FFFFFF", p: 1.2 }}>
                    <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontWeight: 700, fontFamily: G, letterSpacing: "0.05em", textTransform: "uppercase", mb: 0.6 }}>
                      Agent answer
                    </Typography>
                    <Typography sx={{ fontSize: "0.76rem", color: "#5C5650", fontFamily: G, lineHeight: 1.5 }}>
                      Grounded on scoped database matches and relationship links.
                    </Typography>
                    {recommendedAction && (
                      <Box sx={{ mt: 0.8, background: "#F7F5F0", border: "1px solid #E8E3DA", borderRadius: "8px", px: 1, py: 0.7 }}>
                        <Typography sx={{ fontSize: "0.7rem", color: "#17140F", fontFamily: G }}>
                          <strong>Recommended action:</strong> {recommendedAction}
                        </Typography>
                      </Box>
                    )}
                  </Box>
                  <Box sx={{ border: "1px solid #F0CFCF", borderRadius: "10px", background: "#FFF9F9", p: 1.2 }}>
                    <Typography sx={{ fontSize: "0.68rem", color: "#A33A3A", fontWeight: 700, fontFamily: G, letterSpacing: "0.05em", textTransform: "uppercase", mb: 0.6 }}>
                      Agent checks
                    </Typography>
                    <Stack spacing={0.6}>
                      <Box sx={{ background: "#FFF2F2", border: "1px solid #F2C7C7", borderRadius: "8px", px: 1, py: 0.55 }}>
                        <Typography sx={{ fontSize: "0.7rem", color: "#9C3333", fontFamily: G }}>
                          <strong>Missing context:</strong> {scopeActive ? "Low risk" : "Case ID or Vendor ID not provided"}
                        </Typography>
                      </Box>
                      <Box sx={{ background: "#FFF2F2", border: "1px solid #F2C7C7", borderRadius: "8px", px: 1, py: 0.55 }}>
                        <Typography sx={{ fontSize: "0.7rem", color: "#9C3333", fontFamily: G }}>
                          <strong>Wrong assumption:</strong> Avoid taking action without confirming scope IDs
                        </Typography>
                      </Box>
                      {suggestedQs.slice(0, 2).map((q) => (
                        <Box key={q} onClick={() => handleSend(q)} sx={{ background: "#FFFFFF", border: "1px solid #F2C7C7", borderRadius: "8px", px: 1, py: 0.6, cursor: "pointer", transition: "background 0.15s", "&:hover": { background: "#FFF4F4" } }}>
                          <Typography sx={{ fontSize: "0.7rem", color: "#9C3333", fontFamily: G }}>
                            {q}
                          </Typography>
                        </Box>
                      ))}
                    </Stack>
                  </Box>
                </Box>

                {suggestedQs.length > 0 && !isTyping && (
                  <Box sx={{ mb: 1.5 }}>
                    <Typography sx={{ fontSize: "0.66rem", color: "#9C9690", fontFamily: G, mb: 0.7, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                    Suggested follow-ups
                  </Typography>
                  <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.6 }}>
                    {suggestedQs.map((q) => (
                      <Box
                        key={q}
                        onClick={() => handleSend(q)}
                        sx={{
                          background: "#FEF3DC",
                          border: "1px solid #F0C870",
                          borderRadius: "99px",
                          px: 1.4,
                          py: 0.4,
                          fontSize: "0.73rem",
                          fontFamily: G,
                          color: "#A05A10",
                          cursor: "pointer",
                          transition: "all 0.15s",
                          "&:hover": { background: "#F0C870", color: "#17140F" },
                        }}
                      >
                        {q}
                      </Box>
                    ))}
                  </Box>
                </Box>
              )}

              {error && (
                <Alert severity="error" sx={{ mb: 1.5, fontSize: "0.78rem" }} onClose={() => setError(null)}>
                  {error}
                </Alert>
              )}

              <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-end", background: "#F7F5F0", border: "1px solid #D8D2C8", borderRadius: "12px", px: 2, py: 1.2, transition: "border-color 0.15s", "&:focus-within": { borderColor: "#B5742A" } }}>
                <Box
                  component="textarea"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  disabled={isTyping}
                  placeholder={vendorId ? `Ask about Vendor ${vendorId}...` : caseId ? `Ask about Case ${caseId}...` : "Ask about AP/PO data..."}
                  rows={1}
                  sx={{ flex: 1, background: "transparent", border: "none", outline: "none", resize: "none", fontFamily: G, fontSize: "0.875rem", color: "#17140F", lineHeight: 1.55, minHeight: "22px", maxHeight: "120px", overflow: "auto", "::placeholder": { color: "#9C9690" }, "&:disabled": { opacity: 0.5 } }}
                />
                <Button variant="contained" size="small" onClick={() => handleSend()} disabled={!input.trim() || isTyping}
                  sx={{ flexShrink: 0, minWidth: 0, px: 1.5, py: 0.8, borderRadius: "8px" }}>
                  {isTyping
                    ? <CircularProgress size={14} sx={{ color: "#FFFFFF" }} />
                    : <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8h12M10 4l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
                  }
                </Button>
              </Box>
              <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, mt: 0.7, textAlign: "center" }}>
                {scopeActive
                  ? `Scoped search · ${[caseId && `Case ${caseId}`, vendorId && `Vendor ${vendorId}`].filter(Boolean).join(" + ")} · Enter to send`
                  : "Full database context · Enter to send · Shift+Enter for newline"}
              </Typography>
            </Box>
          </Card>
        </Grid>

        <Grid item xs={12} md={4} sx={{ display: "flex", flexDirection: "column" }}>
          <Box sx={{ height: "100%", overflow: "auto", display: "flex", flexDirection: "column" }}>
            {!showRight ? (
              <Card sx={{ flex: 1 }}>
                <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", opacity: 0.4 }}>
                  <Box sx={{ width: 40, height: 40, borderRadius: "10px", background: "#F0EDE6", border: "1px solid #E8E3DA", display: "flex", alignItems: "center", justifyContent: "center", mb: 1.5 }}>
                    <SqlIcon size={18} />
                  </Box>
                  <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, textAlign: "center", maxWidth: 180, lineHeight: 1.6 }}>
                    Agent source details appear here after your first message
                  </Typography>
                </CardContent>
              </Card>
            ) : (
              <>
                <AgentCard agentName={agentUsed} />
                <DataUsedCard dataSources={dataSources} />
              </>
            )}
          </Box>
        </Grid>
      </Grid>
    </div>
  );
}
