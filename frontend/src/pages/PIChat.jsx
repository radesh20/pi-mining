import React, { useState, useEffect, useRef } from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Chip from "@mui/material/Chip";
import Alert from "@mui/material/Alert";
import TextField from "@mui/material/TextField";
import Grid from "@mui/material/Grid";
import CircularProgress from "@mui/material/CircularProgress";
import ReactMarkdown from "react-markdown";
import { sendChatMessage } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const QUICK_QUESTIONS = [
    "What is causing the most delays in our invoice process?",
    "Which exception type has the highest frequency?",
    "What is the main bottleneck and how long does it take?",
    "What does the top agent recommend right now?",
    "Which process variant is most common?",
    "What is the average end-to-end cycle time?",
    "Which vendor has the highest exception rate?",
    "Are there any conformance violations detected?",
];

// Markdown styles applied to the assistant bubble
const MD_SX = {
    "& p": { margin: 0, marginBottom: "8px", "&:last-child": { marginBottom: 0 } },
    "& strong": { fontWeight: 700, color: "#17140F" },
    "& em": { fontStyle: "italic" },
    "& ul, & ol": { paddingLeft: "18px", margin: "6px 0" },
    "& li": { marginBottom: "4px" },
    "& code": { fontFamily: "monospace", fontSize: "0.82rem", background: "#F0EDE6", borderRadius: "4px", padding: "1px 5px" },
    "& h1, & h2, & h3": { fontFamily: S, fontWeight: 400, margin: "8px 0 4px", color: "#17140F" },
};

function SectionHeader({ label, meta }) {
    return (
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 2 }}>
            <Typography sx={{ fontFamily: S, fontSize: "1.1rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.015em", whiteSpace: "nowrap" }}>
                {label}
            </Typography>
            {meta && (
                <Box sx={{ background: "#F0EDE6", border: "1px solid #E8E3DA", borderRadius: "99px", px: 1.2, py: 0.2 }}>
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{meta}</Typography>
                </Box>
            )}
            <Box sx={{ flex: 1, height: "1px", background: "#E8E3DA" }} />
        </Box>
    );
}

function UserMessage({ msg }) {
    return (
        <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{ maxWidth: "68%" }}>
                <Box sx={{
                    background: "#17140F", color: "#F7F5F0",
                    borderRadius: "14px 14px 4px 14px",
                    px: 2, py: 1.5, fontSize: "0.875rem", fontFamily: G,
                    lineHeight: 1.65, whiteSpace: "pre-wrap", wordBreak: "break-word",
                }}>
                    {msg.content}
                </Box>
                <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, mt: 0.5, textAlign: "right" }}>
                    {formatTime(msg.ts)}
                </Typography>
            </Box>
            <Box sx={{
                width: 32, height: 32, borderRadius: "8px",
                background: "#EDE9E1", border: "1px solid #D8D2C8",
                display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>
                <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#5C5650", fontFamily: G }}>ME</Typography>
            </Box>
        </Box>
    );
}

// FIX 1 — ReactMarkdown now applied to assistant replies
function AssistantMessage({ msg }) {
    return (
        <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{
                width: 32, height: 32, borderRadius: "8px",
                background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)",
                display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>
                <svg width="16" height="16" viewBox="0 0 22 22" fill="none">
                    <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.15" stroke="#B5742A" strokeWidth="1.5" />
                    <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
                </svg>
            </Box>
            <Box sx={{ maxWidth: "72%", flex: 1 }}>
                <Box sx={{
                    background: msg.isError ? "#FAEAEA" : "#FFFFFF",
                    border: `1px solid ${msg.isError ? "#E0A0A0" : "#E8E3DA"}`,
                    borderRadius: "4px 14px 14px 14px",
                    px: 2, py: 1.5,
                    fontSize: "0.875rem", fontFamily: G, lineHeight: 1.65,
                    color: msg.isError ? "#B03030" : "#17140F",
                    wordBreak: "break-word",
                    boxShadow: "0 1px 4px rgba(23,20,15,0.06)",
                    ...MD_SX,
                }}>
                    {msg.isError
                        ? msg.content
                        : <ReactMarkdown>{msg.content}</ReactMarkdown>
                    }
                </Box>
                <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, mt: 0.5 }}>
                    {formatTime(msg.ts)}
                </Typography>
            </Box>
        </Box>
    );
}

function TypingIndicator() {
    return (
        <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{
                width: 32, height: 32, borderRadius: "8px",
                background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)",
                display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>
                <svg width="16" height="16" viewBox="0 0 22 22" fill="none">
                    <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.15" stroke="#B5742A" strokeWidth="1.5" />
                    <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
                </svg>
            </Box>
            <Box sx={{
                background: "#FFFFFF", border: "1px solid #E8E3DA",
                borderRadius: "4px 14px 14px 14px", px: 2, py: 1.5,
                display: "flex", gap: "5px", alignItems: "center",
                boxShadow: "0 1px 4px rgba(23,20,15,0.06)",
            }}>
                {[0, 0.18, 0.36].map((delay, i) => (
                    <Box key={i} sx={{
                        width: 6, height: 6, borderRadius: "50%", background: "#B5742A",
                        animation: "chatBounce 1.2s ease-in-out infinite",
                        animationDelay: `${delay}s`,
                    }} />
                ))}
            </Box>
            <style>{`
                @keyframes chatBounce {
                    0%, 80%, 100% { transform: translateY(0); opacity: 0.4; }
                    40% { transform: translateY(-5px); opacity: 1; }
                }
            `}</style>
        </Box>
    );
}

function formatTime(date) {
    return new Date(date).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export default function PIChat() {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState("");
    const [isTyping, setIsTyping] = useState(false);
    const [caseId, setCaseId] = useState("");
    const [vendorId, setVendorId] = useState("");
    const [contextUsed, setContextUsed] = useState(null);
    const [error, setError] = useState(null);
    const messagesEndRef = useRef(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages, isTyping]);

    async function handleSend(text) {
        const msg = (text || input).trim();
        if (!msg || isTyping) return;

        setInput("");
        setError(null);

        const userMsg = { id: Date.now(), role: "user", content: msg, ts: new Date() };
        setMessages((prev) => [...prev, userMsg]);
        setIsTyping(true);

        try {
            const history = messages.slice(-6).map((m) => ({ role: m.role, content: m.content }));
            const result = await sendChatMessage({ message: msg, caseId, vendorId, conversationHistory: history });

            setMessages((prev) => [...prev, {
                id: Date.now() + 1,
                role: "assistant",
                content: result.reply || result.data?.reply || "No response received.",
                ts: new Date(),
                isError: !result.success,
                contextUsed: result.context_used,
            }]);

            if (result.context_used) setContextUsed(result.context_used);
        } catch (err) {
            setMessages((prev) => [...prev, {
                id: Date.now() + 1, role: "assistant",
                content: `Unable to reach the PI backend: ${err.message}`,
                ts: new Date(), isError: true,
            }]);
            setError(err.message);
        } finally {
            setIsTyping(false);
        }
    }

    function handleKeyDown(e) {
        if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
    }

    const scopeActive = caseId || vendorId;

    return (
        <div className="page-container">
            <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                    <Box>
                        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.5 }}>
                            Process Intelligence Chat
                        </Typography>
                        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
                            Ask anything about your AP invoice process — answers grounded in live Celonis data
                        </Typography>
                    </Box>
                    {messages.length > 0 && (
                        <Button variant="outlined" size="small" onClick={() => { setMessages([]); setContextUsed(null); }}>
                            Clear conversation
                        </Button>
                    )}
                </Box>
            </Box>

            <Grid container spacing={3} sx={{ height: "calc(100vh - 220px)", minHeight: 500 }}>

                {/* Left sidebar */}
                <Grid item xs={12} md={3} sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <Card>
                        <CardContent>
                            <SectionHeader label="Scope" />
                            <Stack spacing={1.5}>
                                <TextField label="Case ID (optional)" size="small" fullWidth placeholder="e.g. CASE_0042" value={caseId} onChange={(e) => setCaseId(e.target.value)} />
                                <TextField label="Vendor ID (optional)" size="small" fullWidth placeholder="e.g. LIFNR_001" value={vendorId} onChange={(e) => setVendorId(e.target.value)} />
                                {scopeActive && (
                                    <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.8 }}>
                                        {caseId && <Chip label={`Case: ${caseId}`} size="small" color="info" onDelete={() => setCaseId("")} />}
                                        {vendorId && <Chip label={`Vendor: ${vendorId}`} size="small" color="warning" onDelete={() => setVendorId("")} />}
                                    </Box>
                                )}
                                {!scopeActive && (
                                    <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G }}>
                                        No scope set — answering from global PI data
                                    </Typography>
                                )}
                            </Stack>
                        </CardContent>
                    </Card>

                    <Card sx={{ flex: 1, overflow: "hidden" }}>
                        <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
                            <SectionHeader label="Quick questions" />
                            <Stack spacing={0.8} sx={{ overflow: "auto", flex: 1 }}>
                                {QUICK_QUESTIONS.map((q) => (
                                    <Box key={q} onClick={() => handleSend(q)} sx={{
                                        background: "#F7F5F0", border: "1px solid #E8E3DA",
                                        borderRadius: "8px", px: 1.5, py: 1,
                                        fontSize: "0.78rem", fontFamily: G, color: "#5C5650",
                                        cursor: "pointer", lineHeight: 1.45, transition: "all 0.15s",
                                        "&:hover": { background: "#FEF3DC", borderColor: "#F0C870", color: "#A05A10" },
                                    }}>
                                        {q}
                                    </Box>
                                ))}
                            </Stack>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Main chat */}
                <Grid item xs={12} md={6} sx={{ display: "flex", flexDirection: "column" }}>
                    <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                        <Box sx={{ flex: 1, overflow: "auto", px: 2.5, py: 2.5, display: "flex", flexDirection: "column", gap: 2 }}>
                            {messages.length === 0 ? (
                                <Box sx={{ margin: "auto", textAlign: "center", maxWidth: 360, py: 4 }}>
                                    <Box sx={{
                                        width: 52, height: 52, borderRadius: "14px",
                                        background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.25)",
                                        display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px",
                                    }}>
                                        <svg width="24" height="24" viewBox="0 0 22 22" fill="none">
                                            <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.2" stroke="#B5742A" strokeWidth="1.5" />
                                            <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
                                        </svg>
                                    </Box>
                                    <Typography sx={{ fontFamily: S, fontSize: "1.4rem", color: "#17140F", mb: 1 }}>
                                        Ask your process data anything
                                    </Typography>
                                    <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, lineHeight: 1.65 }}>
                                        Every answer is grounded in your live Celonis process mining data — delays, exceptions, vendors, agents, and variants.
                                    </Typography>
                                </Box>
                            ) : (
                                messages.map((msg) =>
                                    msg.role === "user"
                                        ? <UserMessage key={msg.id} msg={msg} />
                                        : <AssistantMessage key={msg.id} msg={msg} />
                                )
                            )}
                            {isTyping && <TypingIndicator />}
                            <div ref={messagesEndRef} />
                        </Box>

                        <Box sx={{ borderTop: "1px solid #E8E3DA", p: 2 }}>
                            {error && (
                                <Alert severity="error" sx={{ mb: 1.5, fontSize: "0.78rem" }} onClose={() => setError(null)}>
                                    {error}
                                </Alert>
                            )}
                            <Box sx={{
                                display: "flex", gap: 1.5, alignItems: "flex-end",
                                background: "#F7F5F0", border: "1px solid #D8D2C8",
                                borderRadius: "12px", px: 2, py: 1.2,
                                transition: "border-color 0.15s",
                                "&:focus-within": { borderColor: "#B5742A" },
                            }}>
                                <Box
                                    component="textarea"
                                    value={input}
                                    onChange={(e) => setInput(e.target.value)}
                                    onKeyDown={handleKeyDown}
                                    disabled={isTyping}
                                    placeholder="Ask about delays, exceptions, vendors, or specific cases… (Shift+Enter for newline)"
                                    rows={1}
                                    sx={{
                                        flex: 1, background: "transparent", border: "none", outline: "none",
                                        resize: "none", fontFamily: G, fontSize: "0.875rem", color: "#17140F",
                                        lineHeight: 1.55, minHeight: "22px", maxHeight: "120px", overflow: "auto",
                                        "::placeholder": { color: "#9C9690" },
                                        "&:disabled": { opacity: 0.5 },
                                    }}
                                />
                                <Button
                                    variant="contained" size="small" onClick={() => handleSend()}
                                    disabled={!input.trim() || isTyping}
                                    sx={{ flexShrink: 0, minWidth: 0, px: 1.5, py: 0.8, borderRadius: "8px" }}
                                >
                                    {isTyping
                                        ? <CircularProgress size={14} sx={{ color: "#FFFFFF" }} />
                                        : <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                                            <path d="M2 8h12M10 4l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                        </svg>
                                    }
                                </Button>
                            </Box>
                            <Typography sx={{ fontSize: "0.69rem", color: "#9C9690", fontFamily: G, mt: 0.8, textAlign: "center" }}>
                                {scopeActive
                                    ? `Scoped to: ${[caseId && `Case ${caseId}`, vendorId && `Vendor ${vendorId}`].filter(Boolean).join(", ")}`
                                    : "Global PI context · Enter to send · Shift+Enter for newline"}
                            </Typography>
                        </Box>
                    </Card>
                </Grid>

                {/* Right panel */}
                <Grid item xs={12} md={3} sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <Card sx={{ flex: 1, overflow: "hidden" }}>
                        <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
                            <SectionHeader label="Context injected" />
                            {!contextUsed ? (
                                <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
                                    <Typography sx={{ fontSize: "0.78rem", color: "#9C9690", fontFamily: G, textAlign: "center", lineHeight: 1.6 }}>
                                        Ask a question to see which PI data was used to answer it.
                                    </Typography>
                                </Box>
                            ) : (
                                <Stack spacing={1.5} sx={{ overflow: "auto", flex: 1 }}>
                                    {contextUsed.global && (
                                        <ContextBlock
                                            title="Global process" color="#1A6B5E" bgColor="#F2FAF6" borderColor="#B8DFD0"
                                            data={{
                                                "Total cases": contextUsed.global.total_cases,
                                                "Avg cycle": contextUsed.global.avg_end_to_end_days != null ? `${contextUsed.global.avg_end_to_end_days}d` : null,
                                                "Exception rate": contextUsed.global.exception_rate != null ? `${contextUsed.global.exception_rate}%` : null,
                                                "Exception types": contextUsed.global.exception_type_count,
                                                "Violations": contextUsed.global.conformance_count,
                                            }}
                                        />
                                    )}
                                    {contextUsed.agents && (
                                        <ContextBlock
                                            title="Agent intelligence" color="#1E4E8C" bgColor="#EBF2FC" borderColor="#90B8E8"
                                            data={{
                                                "Agents active": contextUsed.agents.agent_count,
                                                "Top agent": contextUsed.agents.top_recommendation?.agent_name,
                                                "Priority": contextUsed.agents.top_recommendation?.priority,
                                            }}
                                        />
                                    )}
                                    {/* FIX 3 — vendor_summary now shown */}
                                    {contextUsed.vendor_summary && (
                                        <ContextBlock
                                            title="Vendor summary" color="#5C5650" bgColor="#F7F5F0" borderColor="#E8E3DA"
                                            data={{
                                                "Total vendors": contextUsed.vendor_summary.total_vendors,
                                                "Highest risk": contextUsed.vendor_summary.highest_exception_vendor?.vendor_id,
                                                "Exception rate": contextUsed.vendor_summary.highest_exception_vendor?.exception_rate != null
                                                    ? `${contextUsed.vendor_summary.highest_exception_vendor.exception_rate}%`
                                                    : null,
                                            }}
                                        />
                                    )}
                                    {contextUsed.case && (
                                        <ContextBlock
                                            title={`Case: ${contextUsed.case.case_id}`} color="#A05A10" bgColor="#FEF3DC" borderColor="#F0C870"
                                            data={{
                                                "Current stage": contextUsed.case.current_stage,
                                                "Days in process": contextUsed.case.days_in_process != null ? `${Number(contextUsed.case.days_in_process).toFixed(1)}d` : null,
                                                "Activities": contextUsed.case.activity_count,
                                            }}
                                        />
                                    )}
                                    {contextUsed.vendor && (
                                        <ContextBlock
                                            title={`Vendor: ${contextUsed.vendor.vendor_id}`} color="#B03030" bgColor="#FAEAEA" borderColor="#E0A0A0"
                                            data={{
                                                "Total cases": contextUsed.vendor.total_cases,
                                                "Exception rate": contextUsed.vendor.exception_rate != null ? `${contextUsed.vendor.exception_rate}%` : null,
                                                "Risk score": contextUsed.vendor.risk_score,
                                            }}
                                        />
                                    )}
                                </Stack>
                            )}
                        </CardContent>
                    </Card>

                    <Card>
                        <CardContent>
                            <SectionHeader label="How it works" />
                            <Stack spacing={1}>
                                {[
                                    ["1", "Question hits POST /api/chat/"],
                                    ["2", "Backend fetches live Celonis data"],
                                    ["3", "PI context injected into system prompt"],
                                    ["4", "Azure GPT-4o reasons over real data"],
                                    ["5", "Grounded answer returned"],
                                ].map(([n, text]) => (
                                    <Box key={n} sx={{ display: "flex", alignItems: "flex-start", gap: 1 }}>
                                        <Box sx={{
                                            width: 20, height: 20, borderRadius: "50%",
                                            background: "#F0EDE6", border: "1px solid #D8D2C8",
                                            display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                                        }}>
                                            <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: "#B5742A", fontFamily: G }}>{n}</Typography>
                                        </Box>
                                        <Typography sx={{ fontSize: "0.75rem", color: "#5C5650", fontFamily: G, lineHeight: 1.5, pt: "2px" }}>
                                            {text}
                                        </Typography>
                                    </Box>
                                ))}
                            </Stack>
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </div>
    );
}

function ContextBlock({ title, color, bgColor, borderColor, data }) {
    const entries = Object.entries(data).filter(([, v]) => v != null);
    if (!entries.length) return null;
    return (
        <Box sx={{ background: bgColor, border: `1px solid ${borderColor}`, borderRadius: "10px", p: 1.5 }}>
            <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color, fontFamily: G, mb: 1 }}>
                {title}
            </Typography>
            {entries.map(([k, v]) => (
                <Box key={k} sx={{
                    display: "flex", justifyContent: "space-between", alignItems: "baseline",
                    py: 0.4, borderBottom: `1px solid ${borderColor}`,
                    "&:last-child": { borderBottom: "none" },
                }}>
                    <Typography sx={{ fontSize: "0.72rem", color: "#9C9690", fontFamily: G }}>{k}</Typography>
                    <Typography sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>{String(v)}</Typography>
                </Box>
            ))}
        </Box>
    );
}