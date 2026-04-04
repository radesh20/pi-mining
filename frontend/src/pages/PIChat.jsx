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
import Divider from "@mui/material/Divider";
import ReactMarkdown from "react-markdown";
import { sendChatMessage } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";


// ── Agent config ──────────────────────────────────────────────────────────────

const AGENT_STYLES = {
    "Vendor Intelligence Agent": { dot: "#185FA5", bg: "#EBF4FD", border: "#B5D4F4", text: "#185FA5" },
    "Invoice Processing Agent": { dot: "#1A6B5E", bg: "#F2FAF6", border: "#B8DFD0", text: "#1A6B5E" },
    "Exception Detection Agent": { dot: "#B03A1A", bg: "#FEE8E0", border: "#F5A58A", text: "#B03A1A" },
    "Conformance Checker Agent": { dot: "#A05A10", bg: "#FEF3DC", border: "#F0C870", text: "#A05A10" },
    "Process Insight Agent": { dot: "#B5742A", bg: "rgba(181,116,42,0.08)", border: "rgba(181,116,42,0.3)", text: "#B5742A" },
    "Case Resolution Agent": { dot: "#1E4E8C", bg: "#EBF2FC", border: "#90B8E8", text: "#1E4E8C" },
    "Process Intelligence Agent": { dot: "#B5742A", bg: "rgba(181,116,42,0.08)", border: "rgba(181,116,42,0.3)", text: "#B5742A" },
};

const AGENT_DESCRIPTIONS = {
    "Vendor Intelligence Agent": "Analyses vendor-specific event log patterns, exception rates, and cycle time deviations",
    "Invoice Processing Agent": "Traces individual invoice cases through the event log to identify stage delays",
    "Exception Detection Agent": "Scans event log for exception patterns, triggers, and resolution paths",
    "Conformance Checker Agent": "Validates process paths against expected flow rules in the event log",
    "Process Insight Agent": "Computes cycle times, bottlenecks, and throughput from end-to-end event data",
    "Case Resolution Agent": "Generates next-best-action recommendations based on case and variant data",
    "Process Intelligence Agent": "Orchestrates multi-source Celonis event log analysis for process answers",
};

// ── Risk helper ───────────────────────────────────────────────────────────────

function getRisk(excRate, overallExc, avgDur, overallDur) {
    const isCritical = excRate >= 60 || (avgDur > 0 && overallDur > 0 && avgDur >= overallDur * 2);
    const isHigh = !isCritical && (excRate >= 40 || (avgDur > overallDur + 5));
    const isMedium = !isCritical && !isHigh && (excRate >= 20 || (avgDur > overallDur + 2));
    const label = isCritical ? "CRITICAL" : isHigh ? "HIGH" : isMedium ? "MEDIUM" : "LOW";
    const color = isCritical ? "#B03A1A" : isHigh ? "#B5742A" : isMedium ? "#185FA5" : "#1A6B5E";
    const bg = isCritical ? "#FEE8E0" : isHigh ? "#FEF3DC" : isMedium ? "#EBF4FD" : "#F2FAF6";
    const border = isCritical ? "#F5A58A" : isHigh ? "#F0C870" : isMedium ? "#B5D4F4" : "#B8DFD0";
    return { label, color, bg, border };
}

// ── Shared helpers ────────────────────────────────────────────────────────────

function PIIcon({ size = 16 }) {
    return (
        <svg width={size} height={size} viewBox="0 0 22 22" fill="none">
            <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.15" stroke="#B5742A" strokeWidth="1.5" />
            <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
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

// ── Chat messages ─────────────────────────────────────────────────────────────

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
                <PIIcon size={14} />
            </Box>
            <Box sx={{ maxWidth: "80%", flex: 1 }}>
                {/* Scope badge */}
                {msg.scopeLabel && (
                    <Box sx={{ mb: 0.6 }}>
                        <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1, py: 0.15, display: "inline-flex", alignItems: "center", gap: 0.5 }}>
                            <svg width="8" height="8" viewBox="0 0 9 9" fill="none">
                                <circle cx="4.5" cy="4.5" r="3.5" stroke="#B5742A" strokeWidth="1.2" />
                                <circle cx="4.5" cy="4.5" r="1.5" fill="#B5742A" />
                            </svg>
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
                <PIIcon size={14} />
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

// ── Right panel components ────────────────────────────────────────────────────

/** Agent card — shows which agent answered and what it does */
function AgentCard({ agentName }) {
    if (!agentName) return null;
    const st = AGENT_STYLES[agentName] || AGENT_STYLES["Process Intelligence Agent"];
    const desc = AGENT_DESCRIPTIONS[agentName] || "";
    return (
        <Card sx={{ mb: 1.5, background: st.bg, border: `1px solid ${st.border}` }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 0.8 }}>
                    <Box sx={{ width: 8, height: 8, borderRadius: "50%", background: st.dot, flexShrink: 0 }} />
                    <Typography sx={{ fontSize: "0.69rem", fontWeight: 700, color: st.text, fontFamily: G, letterSpacing: "0.04em", textTransform: "uppercase" }}>
                        Agent used
                    </Typography>
                </Box>
                <Typography sx={{ fontSize: "0.82rem", fontWeight: 700, color: "#17140F", fontFamily: G, mb: 0.5, lineHeight: 1.3 }}>
                    {agentName}
                </Typography>
                <Typography sx={{ fontSize: "0.7rem", color: "#5C5650", fontFamily: G, lineHeight: 1.5 }}>
                    {desc}
                </Typography>
            </CardContent>
        </Card>
    );
}

/** Vendor analysis card — fixed field mapping */
function VendorCard({ vendorData, vendorId }) {
    if (!vendorId) return null;

    const hasData = !!vendorData && vendorData.total_cases != null;
    // Use correct field names from chat_service.py _build_vendor_snapshot
    const excRate = parseFloat(vendorData?.exception_rate_pct ?? 0);
    const overallExc = parseFloat(vendorData?.overall_exception_rate_pct ?? 0);
    const avgDur = parseFloat(vendorData?.avg_duration_days ?? 0);
    const overallDur = parseFloat(vendorData?.overall_avg_duration_days ?? 0);
    const excDiff = (excRate - overallExc).toFixed(1);
    const durDiff = (avgDur - overallDur).toFixed(1);

    const { label: riskLabel, color: riskColor, bg: riskBg, border: riskBorder } =
        getRisk(excRate, overallExc, avgDur, overallDur);

    return (
        <Card sx={{ mb: 1.5, background: hasData ? riskBg : "#F7F5F0", border: `1.5px solid ${hasData ? riskBorder : "#E8E3DA"}` }}>
            <CardContent sx={{ pb: "12px !important" }}>
                {/* Header */}
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", mb: 1.2 }}>
                    <Box>
                        <Typography sx={{ fontFamily: G, fontSize: "0.72rem", fontWeight: 700, color: "#9C9690", letterSpacing: "0.06em", textTransform: "uppercase" }}>
                            Vendor
                        </Typography>
                        <Typography sx={{ fontFamily: G, fontSize: "0.9rem", fontWeight: 700, color: "#17140F", mt: 0.2 }}>
                            {vendorId}
                        </Typography>
                        <Typography sx={{ fontSize: "0.62rem", color: "#9C9690", fontFamily: G }}>
                            from Celonis event log
                        </Typography>
                    </Box>
                    {hasData && (
                        <Box sx={{ background: riskColor, borderRadius: "5px", px: 1, py: 0.3 }}>
                            <Typography sx={{ fontSize: "0.62rem", fontWeight: 800, color: "#fff", fontFamily: G, letterSpacing: "0.06em" }}>
                                {riskLabel}
                            </Typography>
                        </Box>
                    )}
                </Box>

                {!hasData ? (
                    <Typography sx={{ fontSize: "0.7rem", color: "#9C9690", fontFamily: G, textAlign: "center", py: 1 }}>
                        Ask a question to load vendor data
                    </Typography>
                ) : (
                    <>
                        {/* Key stats */}
                        <Box sx={{ background: "rgba(255,255,255,0.7)", borderRadius: "8px", px: 1.2, py: 0.4, mb: 1 }}>
                            {[
                                ["Total invoices", vendorData.total_cases],
                                ["Exception rate", excRate != null ? `${excRate}%` : "—", excRate > overallExc + 5],
                                ["Avg cycle time", avgDur != null ? `${avgDur} days` : "—", avgDur > overallDur + 2],
                                ["Payment terms", vendorData.payment_terms || "—"],
                                ["Currency", vendorData.currency || "—"],
                            ].map(([label, value, highlight]) => (
                                <Box key={label} sx={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", py: 0.4, borderBottom: "1px solid rgba(0,0,0,0.05)", "&:last-child": { borderBottom: "none" } }}>
                                    <Typography sx={{ fontSize: "0.7rem", color: "#6B6560", fontFamily: G }}>{label}</Typography>
                                    <Typography sx={{ fontSize: "0.73rem", fontWeight: 700, color: highlight ? riskColor : "#17140F", fontFamily: G }}>
                                        {value ?? "—"}
                                    </Typography>
                                </Box>
                            ))}
                        </Box>

                        {/* vs global comparison */}
                        <Box sx={{ display: "flex", gap: 0.8, mb: 1 }}>
                            <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "8px", px: 1, py: 0.7, textAlign: "center" }}>
                                <Typography sx={{ fontSize: "0.58rem", color: "#9C9690", fontFamily: G, mb: 0.1 }}>vs avg exception</Typography>
                                <Typography sx={{ fontSize: "0.82rem", fontWeight: 800, color: riskColor, fontFamily: G }}>
                                    {excDiff > 0 ? `+${excDiff}` : excDiff}%
                                </Typography>
                                <Typography sx={{ fontSize: "0.58rem", color: "#9C9690", fontFamily: G }}>avg {overallExc}%</Typography>
                            </Box>
                            <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "8px", px: 1, py: 0.7, textAlign: "center" }}>
                                <Typography sx={{ fontSize: "0.58rem", color: "#9C9690", fontFamily: G, mb: 0.1 }}>vs avg cycle</Typography>
                                <Typography sx={{ fontSize: "0.82rem", fontWeight: 800, color: riskColor, fontFamily: G }}>
                                    {durDiff > 0 ? `+${durDiff}` : durDiff}d
                                </Typography>
                                <Typography sx={{ fontSize: "0.58rem", color: "#9C9690", fontFamily: G }}>avg {overallDur}d</Typography>
                            </Box>
                        </Box>

                        {/* Exception types */}
                        {vendorData.top_exception_types?.length > 0 && (
                            <Box sx={{ mb: 1 }}>
                                <Typography sx={{ fontSize: "0.62rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.5, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                                    Exception types
                                </Typography>
                                <Stack spacing={0.4}>
                                    {vendorData.top_exception_types.map((e, i) => (
                                        <Box key={i} sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 1, py: 0.4 }}>
                                            <Typography sx={{ fontSize: "0.67rem", color: "#3C3830", fontFamily: G }}>{e.exception_type}</Typography>
                                            <Typography sx={{ fontSize: "0.67rem", fontWeight: 700, color: riskColor, fontFamily: G }}>{e.case_count} cases</Typography>
                                        </Box>
                                    ))}
                                </Stack>
                            </Box>
                        )}

                        {/* Most common variant */}
                        {vendorData.most_common_variant && (
                            <Box>
                                <Typography sx={{ fontSize: "0.62rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.4, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                                    Most common path
                                </Typography>
                                <Box sx={{ background: "rgba(255,255,255,0.65)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 1, py: 0.6 }}>
                                    <Typography sx={{ fontSize: "0.65rem", color: "#3C3830", fontFamily: G, lineHeight: 1.5, wordBreak: "break-word" }}>
                                        {vendorData.most_common_variant}
                                    </Typography>
                                </Box>
                            </Box>
                        )}
                    </>
                )}
            </CardContent>
        </Card>
    );
}

/** PI Data Used — shows what data powered the answer */
function PIDataUsedCard({ dataSources }) {
    if (!dataSources || dataSources.length === 0) return null;

    const enriched = dataSources.map((src) => {
        const s = src.toLowerCase();
        if (s.includes("event log") && s.includes("cases")) {
            const m = src.match(/(\d[\d,]*)\s+cases/i);
            const e = src.match(/(\d[\d,]*)\s+events/i);
            return { icon: "📊", label: "Celonis Event Log", detail: `${m?.[1] || "?"} cases · ${e?.[1] || "?"} events mined`, bg: "#F2FAF6", border: "#B8DFD0", lc: "#1A6B5E" };
        }
        if (s.includes("variant")) {
            const m = src.match(/(\d+)\s+distinct/i);
            return { icon: "🔀", label: "Variant Analysis", detail: m ? `${m[1]} distinct process paths discovered` : src.split("—")[1]?.trim() || src, bg: "#EBF4FD", border: "#B5D4F4", lc: "#185FA5" };
        }
        if (s.includes("exception pattern")) {
            const m = src.match(/(\d+)\s+type/i);
            return { icon: "⚠️", label: "Exception Patterns", detail: m ? `${m[1]} exception types from event log` : src.split("—")[1]?.trim() || src, bg: "#FEF3DC", border: "#F0C870", lc: "#A05A10" };
        }
        if (s.includes("conformance")) {
            const m = src.match(/(\d+)\s+violation/i);
            return { icon: "🚨", label: "Conformance Analysis", detail: m ? `${m[1]} process rule violations found` : src.split("—")[1]?.trim() || src, bg: "#FEE8E0", border: "#F5A58A", lc: "#B03A1A" };
        }
        if (s.includes("vendor") && !s.includes("case")) {
            const m = src.match(/vendor\s+(\w+)/i);
            return { icon: "🏢", label: "Vendor Data", detail: src.split("—")[1]?.trim() || src, bg: "#F0EDE6", border: "#D8D2C8", lc: "#5C5650" };
        }
        if (s.includes("case") && s.includes("event trace")) {
            return { icon: "📋", label: "Case Event Trace", detail: src.split("—")[1]?.trim() || src, bg: "#FEF3DC", border: "#F0C870", lc: "#A05A10" };
        }
        if (s.includes("similar cases")) {
            return { icon: "🔗", label: "Similar Cases", detail: src.split("—")[1]?.trim() || src, bg: "#EBF4FD", border: "#B5D4F4", lc: "#185FA5" };
        }
        return { icon: "📌", label: src.split("—")[0].trim(), detail: src.split("—")[1]?.trim() || "", bg: "#F7F5F0", border: "#E8E3DA", lc: "#5C5650" };
    });

    return (
        <Card sx={{ mb: 1.5 }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <SectionHeader label="PI data used" count={dataSources.length} />
                <Stack spacing={0.7}>
                    {enriched.map((e, i) => (
                        <Box key={i} sx={{ display: "flex", alignItems: "flex-start", gap: 0.9, background: e.bg, border: `1px solid ${e.border}`, borderRadius: "8px", px: 1.1, py: 0.8 }}>
                            <Typography sx={{ fontSize: "13px", flexShrink: 0, mt: "1px", lineHeight: 1 }}>{e.icon}</Typography>
                            <Box>
                                <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: e.lc, fontFamily: G, lineHeight: 1.2 }}>{e.label}</Typography>
                                {e.detail && (
                                    <Typography sx={{ fontSize: "0.65rem", color: "#5C5650", fontFamily: G, lineHeight: 1.4, mt: 0.15 }}>{e.detail}</Typography>
                                )}
                            </Box>
                        </Box>
                    ))}
                </Stack>
                <Box sx={{ mt: 1, px: 1, py: 0.5, background: "#F7F5F0", borderRadius: "6px", border: "1px solid #E8E3DA" }}>
                    <Typography sx={{ fontSize: "0.63rem", color: "#9C9690", fontFamily: G }}>
                        Live from Celonis event log — not static rules or cached queries
                    </Typography>
                </Box>
            </CardContent>
        </Card>
    );
}

/** Similar Cases card */
function SimilarCasesCard({ similarCases, currentCaseId }) {
    if (!similarCases || similarCases.length === 0) return null;
    return (
        <Card sx={{ mb: 1.5 }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <SectionHeader label="Similar cases" count={similarCases.length} />
                <Typography sx={{ fontSize: "0.68rem", color: "#9C9690", fontFamily: G, mb: 1, lineHeight: 1.4 }}>
                    Same process path as Case {currentCaseId}
                </Typography>
                <Stack spacing={0.6}>
                    {similarCases.map((sc) => (
                        <Box key={sc.case_id} sx={{ background: "#FAFAF8", border: "1px solid #E8E3DA", borderRadius: "8px", px: 1.2, py: 0.8, transition: "border-color 0.15s", "&:hover": { borderColor: "#B5742A" } }}>
                            <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                <Typography sx={{ fontSize: "0.74rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>{sc.case_id}</Typography>
                                {sc.duration_days != null && (
                                    <Typography sx={{ fontSize: "0.67rem", color: "#9C9690", fontFamily: G }}>{sc.duration_days}d</Typography>
                                )}
                            </Box>
                            {sc.current_stage && (
                                <Typography sx={{ fontSize: "0.67rem", color: "#5C5650", fontFamily: G, mt: 0.2 }}>{sc.current_stage}</Typography>
                            )}
                        </Box>
                    ))}
                </Stack>
            </CardContent>
        </Card>
    );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function PIChat() {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState("");
    const [isTyping, setIsTyping] = useState(false);

    const [contextUsed, setContextUsed] = useState(null);
    const [agentUsed, setAgentUsed] = useState("");
    const [dataSources, setDataSources] = useState([]);
    const [suggestedQs, setSuggestedQs] = useState([]);
    const [error, setError] = useState(null);
    const messagesEndRef = useRef(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages, isTyping]);


    const hasReplied = messages.some((m) => m.role === "assistant" && !m.isError);
    const showRight = dataSources.length > 0 || hasReplied;

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
            const result = await sendChatMessage({ message: msg, conversationHistory: history });

            setMessages((prev) => [...prev, {
                id: Date.now() + 1,
                role: "assistant",
                content: result.reply || "No response received.",
                ts: new Date(),
                isError: !result.success,
                scopeLabel: result.scope_label || "",
            }]);

            if (result.context_used) setContextUsed(result.context_used);
            if (result.agent_used) setAgentUsed(result.agent_used);
            if (result.data_sources) setDataSources(result.data_sources);
            if (result.suggested_questions) setSuggestedQs(result.suggested_questions);

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



    return (
        <div className="page-container">
            {/* Header */}
            <Box sx={{ pt: 4, pb: 3, borderBottom: "1px solid #E8E3DA", mb: 3 }}>
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                    <Box>
                        <Typography sx={{ fontFamily: S, fontSize: "2.2rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.5 }}>
                            Process Intelligence Chat
                        </Typography>
                        <Typography sx={{ fontSize: "0.875rem", color: "#9C9690", fontFamily: G }}>
                            Ask anything about your AP invoice process — answers grounded in live Celonis event log data
                        </Typography>
                    </Box>
                    {messages.length > 0 && (
                        <Button variant="outlined" size="small" onClick={() => {
                            setMessages([]); setContextUsed(null); setAgentUsed("");
                            setDataSources([]); setSuggestedQs([]);
                        }}>
                            Clear conversation
                        </Button>
                    )}
                </Box>
            </Box>

            <Grid container spacing={3} sx={{ height: "calc(100vh - 220px)", minHeight: 500 }}>



                {/* Chat */}
                <Grid item xs={12} md={7} sx={{ display: "flex", flexDirection: "column" }}>
                    <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                        <Box sx={{ flex: 1, overflow: "auto", px: 2.5, py: 2.5, display: "flex", flexDirection: "column", gap: 2 }}>
                            {messages.length === 0 ? (
                                <Box sx={{ margin: "auto", textAlign: "center", maxWidth: 380, py: 4 }}>
                                    <Box sx={{ width: 52, height: 52, borderRadius: "14px", background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.25)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}>
                                        <PIIcon size={24} />
                                    </Box>
                                    <Typography sx={{ fontFamily: S, fontSize: "1.4rem", color: "#17140F", mb: 1 }}>
                                        Ask your process data anything
                                    </Typography>
                                    <Typography sx={{ fontSize: "0.82rem", color: "#9C9690", fontFamily: G, lineHeight: 1.65, mb: 2 }}>
                                        Answers grounded in live Celonis event log — cycle times, variants, exceptions, conformance violations.
                                    </Typography>
                                    <Box sx={{ display: "flex", justifyContent: "center", gap: 1, flexWrap: "wrap" }}>
                                        {["Event log mining", "Variant analysis", "Conformance checking"].map((t) => (
                                            <Box key={t} sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1.2, py: 0.4 }}>
                                                <Typography sx={{ fontSize: "0.72rem", color: "#5C5650", fontFamily: G }}>{t}</Typography>
                                            </Box>
                                        ))}
                                    </Box>
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
                            {/* Suggested follow-ups */}
                            {suggestedQs.length > 0 && !isTyping && (
                                <Box sx={{ mb: 1.5 }}>
                                    <Typography sx={{ fontSize: "0.66rem", color: "#9C9690", fontFamily: G, mb: 0.7, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                                        Suggested follow-ups
                                    </Typography>
                                    <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.6 }}>
                                        {suggestedQs.map((q) => (
                                            <Box key={q} onClick={() => handleSend(q)} sx={{
                                                background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "99px",
                                                px: 1.4, py: 0.4, fontSize: "0.73rem", fontFamily: G, color: "#A05A10",
                                                cursor: "pointer", transition: "all 0.15s",
                                                "&:hover": { background: "#F0C870", color: "#17140F" },
                                            }}>
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
                                <Box component="textarea" value={input} onChange={(e) => setInput(e.target.value)}
                                    onKeyDown={handleKeyDown} disabled={isTyping}
                                    placeholder="Ask about delays, variants, exceptions, conformance…"
                                    rows={1} sx={{ flex: 1, background: "transparent", border: "none", outline: "none", resize: "none", fontFamily: G, fontSize: "0.875rem", color: "#17140F", lineHeight: 1.55, minHeight: "22px", maxHeight: "120px", overflow: "auto", "::placeholder": { color: "#9C9690" }, "&:disabled": { opacity: 0.5 } }}
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
                                Full event log context · Enter to send · Shift+Enter for newline
                            </Typography>
                        </Box>
                    </Card>
                </Grid>

                {/* Right panel */}
                <Grid item xs={12} md={5} sx={{ display: "flex", flexDirection: "column" }}>
                    <Box sx={{ height: "100%", overflow: "auto", display: "flex", flexDirection: "column" }}>
                        {!showRight ? (
                            <Card sx={{ flex: 1 }}>
                                <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", opacity: 0.4 }}>
                                    <Box sx={{ width: 40, height: 40, borderRadius: "10px", background: "#F0EDE6", border: "1px solid #E8E3DA", display: "flex", alignItems: "center", justifyContent: "center", mb: 1.5 }}>
                                        <PIIcon size={18} />
                                    </Box>
                                    <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, textAlign: "center", maxWidth: 180, lineHeight: 1.6 }}>
                                        Agent, data sources, and vendor analysis appear here after your first message
                                    </Typography>
                                </CardContent>
                            </Card>
                        ) : (
                            <>
                                {/* 1 — Agent used */}
                                <AgentCard agentName={agentUsed} />

                                {/* 2 — PI data used */}
                                {dataSources.length > 0 && (
                                    <PIDataUsedCard dataSources={dataSources} />
                                )}
                            </>
                        )}
                    </Box>
                </Grid>
            </Grid>
        </div>
    );
}
