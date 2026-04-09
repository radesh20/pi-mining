import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Alert from "@mui/material/Alert";
import Grid from "@mui/material/Grid";
import CircularProgress from "@mui/material/CircularProgress";
import Collapse from "@mui/material/Collapse";
import Tooltip from "@mui/material/Tooltip";
import ReactMarkdown from "react-markdown";
import { sendChatMessage } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

const AGENT_META = {
    "Process Intelligence Agent": { icon: "⬡", color: "#B5742A", bg: "rgba(181,116,42,0.10)", border: "rgba(181,116,42,0.30)" },
    "Process Insight Agent": { icon: "⏱", color: "#185FA5", bg: "rgba(24,95,165,0.09)", border: "rgba(24,95,165,0.28)" },
    "Exception Detection Agent": { icon: "⚠", color: "#B03030", bg: "rgba(176,48,48,0.09)", border: "rgba(176,48,48,0.28)" },
    "Vendor Intelligence Agent": { icon: "🏭", color: "#1A6B5E", bg: "rgba(26,107,94,0.09)", border: "rgba(26,107,94,0.28)" },
    "Conformance Checker Agent": { icon: "✓", color: "#6B3FA0", bg: "rgba(107,63,160,0.09)", border: "rgba(107,63,160,0.28)" },
    "Invoice Processing Agent": { icon: "📄", color: "#555", bg: "rgba(85,85,85,0.09)", border: "rgba(85,85,85,0.28)" },
    "Case Resolution Agent": { icon: "🔁", color: "#A05A10", bg: "rgba(160,90,16,0.09)", border: "rgba(160,90,16,0.28)" },
};
const getAgentMeta = (name) => AGENT_META[name] || AGENT_META["Process Intelligence Agent"];

function PIIcon({ size = 16 }) {
    return (
        <svg width={size} height={size} viewBox="0 0 22 22" fill="none">
            <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.15" stroke="#B5742A" strokeWidth="1.5" />
            <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
        </svg>
    );
}

const formatTime = (d) => new Date(d).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

const parseProcessPath = (piEvidence) => {
    const raw = piEvidence?.process_path_detected || "";
    if (!raw) return [];
    const parts = raw.includes(" → ") ? raw.split(" → ") : raw.split("→");
    return parts.map(s => s.trim()).filter(Boolean);
};

const STATUS_COLORS = {
    critical: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0", dot: "#C94040" },
    above_benchmark: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870", dot: "#C47020" },
    above_target: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870", dot: "#C47020" },
    normal: { bg: "#E8F5E9", color: "#2E7D32", border: "#A5D6A7", dot: "#43A047" },
};
const CONFIDENCE_COLORS = {
    high: { bg: "#E8F5E9", color: "#2E7D32", border: "#A5D6A7", label: "High confidence" },
    medium: { bg: "#FEF3DC", color: "#A05A10", border: "#F0C870", label: "Medium confidence" },
    low: { bg: "#FAEAEA", color: "#B03030", border: "#E0A0A0", label: "Low confidence" },
};

function getRisk(excRate, overallExc, avgDur, overallDur) {
    const isCritical = excRate >= 60 || (avgDur > 0 && overallDur > 0 && avgDur >= overallDur * 2);
    const isHigh = !isCritical && (excRate >= 40 || avgDur > overallDur + 5);
    const isMedium = !isCritical && !isHigh && (excRate >= 20 || avgDur > overallDur + 2);
    const label = isCritical ? "CRITICAL" : isHigh ? "HIGH" : isMedium ? "MEDIUM" : "LOW";
    const color = isCritical ? "#B03A1A" : isHigh ? "#B5742A" : isMedium ? "#185FA5" : "#1A6B5E";
    const bg = isCritical ? "#FEE8E0" : isHigh ? "#FEF3DC" : isMedium ? "#EBF4FD" : "#F2FAF6";
    const border = isCritical ? "#F5A58A" : isHigh ? "#F0C870" : isMedium ? "#B5D4F4" : "#B8DFD0";
    return { label, color, bg, border };
}

// ── Process Graph ─────────────────────────────────────────────────────────────
function isBottleneckNode(stepLabel, bottleneckActivity) {
    if (!bottleneckActivity) return false;
    return stepLabel.toLowerCase().includes(bottleneckActivity.toLowerCase());
}
function getNodeTheme(label, isBottleneck) {
    const l = label.toLowerCase();
    if (isBottleneck) return { bg: "#FFF0ED", border: "#D95F45", borderStrong: "#C04A32", badgeBg: "#D95F45", text: "#5C1A00", subText: "#B03030", barColor: "#D95F45", numBg: "#D95F45", tag: "BOTTLENECK", tagBg: "#D95F45", tagText: "#fff" };
    if (/(exception|block|due date|hold|park|reject)/.test(l)) return { bg: "#FFF5F5", border: "#F0A0A0", borderStrong: "#D95F45", badgeBg: "#E05050", text: "#6B1A1A", subText: "#B03030", barColor: "#E05050", numBg: "#E05050", tag: "EXCEPTION", tagBg: "#FEE8E0", tagText: "#C04040" };
    if (/(received|start|creat)/.test(l)) return { bg: "#F2FBF5", border: "#7ACC90", borderStrong: "#43A047", badgeBg: "#43A047", text: "#1B4A22", subText: "#2E7D32", barColor: "#43A047", numBg: "#43A047", tag: "START", tagBg: "#E8F5E9", tagText: "#2E7D32" };
    if (/(moved|end|complet|post|paid)/.test(l)) return { bg: "#F0F7FF", border: "#90C4F0", borderStrong: "#1E88E5", badgeBg: "#1E88E5", text: "#0D3060", subText: "#1565C0", barColor: "#1E88E5", numBg: "#1E88E5", tag: "END", tagBg: "#E3F2FD", tagText: "#1565C0" };
    return { bg: "#FAFAF7", border: "#C8C0B0", borderStrong: "#A08060", badgeBg: "#A08060", text: "#3C2800", subText: "#6C5840", barColor: "#A08060", numBg: "#A08060", tag: "STEP", tagBg: "#F0EDE6", tagText: "#7C6850" };
}
function wrapText(label, maxChars = 18) {
    const words = label.split(" "); const lines = []; let cur = "";
    for (const w of words) {
        if ((cur + " " + w).trim().length > maxChars && cur) { lines.push(cur); cur = w; }
        else cur = (cur + " " + w).trim();
    }
    if (cur) lines.push(cur);
    return lines;
}

function ProcessGraph({ piEvidence, pathLabel }) {
    const steps = useMemo(() => parseProcessPath(piEvidence), [piEvidence]);
    const [hov, setHov] = useState(null);
    if (!steps || steps.length === 0) return null;

    const bottleneckActivity = piEvidence?.bottleneck_stage?.activity || "";
    const bottleneckDays = piEvidence?.bottleneck_stage?.avg_days ?? null;
    const bottleneckPct = piEvidence?.bottleneck_stage?.pct_of_total ?? null;
    const goldenPct = piEvidence?.golden_path_percentage ?? null;
    const confidence = piEvidence?.data_completeness?.confidence ?? null;
    const timespan = piEvidence?.event_log_timespan ?? null;

    const metricsUsed = piEvidence?.metrics_used || [];
    const getMetric = (label) => metricsUsed.find(m => m.label === label);

    const cycleMetric = getMetric("Avg Cycle Time") || getMetric("Case Cycle Time");
    const excMetric = getMetric("Exception Rate");
    const bnMetric = getMetric("Bottleneck Duration");
    const caseMetric = getMetric("Cases Analyzed");

    const avgE2E = cycleMetric?.value || null;
    const excRateRaw = excMetric?.value || null;
    const totalCases = caseMetric ? parseInt(caseMetric.value) : (piEvidence?.data_completeness?.cases_analyzed ?? null);

    const e2eDays = avgE2E ? parseFloat(avgE2E) : null;
    const excRateNum = excRateRaw ? parseFloat(excRateRaw) / 100 : null;

    const NODE_W = 310, NODE_H = 96, ARROW_H = 60, PAD_X = 20, PAD_Y = 28, SIDE_W = 80;
    const SVG_W = NODE_W + PAD_X * 2 + SIDE_W;
    const totalH = PAD_Y * 2 + steps.length * NODE_H + (steps.length - 1) * ARROW_H;
    const nodeCX = PAD_X + NODE_W / 2;

    const confColor = confidence === "high" ? { bg: "#E8F5E9", border: "#A5D6A7", text: "#2E7D32" }
        : confidence === "medium" ? { bg: "#FEF3DC", border: "#F0C870", text: "#A05A10" }
            : { bg: "#FAEAEA", border: "#E0A0A0", text: "#B03030" };

    const sideMetrics = [
        cycleMetric && { label: "E2E", value: cycleMetric.value, bg: "#FEF3DC", stroke: "#F0C870", tc: "#7A4010" },
        excMetric && { label: "EXC", value: excMetric.value, bg: "#FAEAEA", stroke: "#E0A0A0", tc: "#8B1A1A" },
        bnMetric && { label: "BN", value: bnMetric.value, bg: "#FEE8E0", stroke: "#F5A58A", tc: "#8B2010" },
        caseMetric && { label: "CASES", value: caseMetric.value, bg: "#F0EDE6", stroke: "#D8D2C8", tc: "#3C2800" },
    ].filter(Boolean);

    return (
        <Box>
            <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 1, mb: 1.5, pb: 1.2, borderBottom: "1px solid #EDE8DE" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <Box sx={{ width: 3, height: 16, borderRadius: "2px", background: "#B5742A" }} />
                    <Typography sx={{ fontSize: "0.72rem", fontWeight: 800, color: "#3C2800", fontFamily: G, textTransform: "uppercase", letterSpacing: "0.09em" }}>
                        {pathLabel || "Process Flow Graph"}
                    </Typography>
                    {confidence && (
                        <Box sx={{ background: confColor.bg, border: `1px solid ${confColor.border}`, borderRadius: "99px", px: 0.9, py: 0.15 }}>
                            <Typography sx={{ fontSize: "0.58rem", fontWeight: 700, color: confColor.text, fontFamily: G }}>{confidence.toUpperCase()} CONFIDENCE</Typography>
                        </Box>
                    )}
                </Box>
                <Box sx={{ display: "flex", gap: 0.7, flexWrap: "wrap", alignItems: "center" }}>
                    {goldenPct != null && (
                        <Box sx={{ background: "#F0FBF7", border: "1px solid #8FCFC5", borderRadius: "6px", px: 0.9, py: 0.2, display: "flex", alignItems: "center", gap: 0.5 }}>
                            <Box sx={{ width: 6, height: 6, borderRadius: "50%", background: "#43A047" }} />
                            <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#1A6B5E", fontFamily: G }}>{goldenPct}% follow golden path</Typography>
                        </Box>
                    )}
                    {totalCases != null && (
                        <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "6px", px: 0.9, py: 0.2 }}>
                            <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#5C4020", fontFamily: G }}>{totalCases} cases</Typography>
                        </Box>
                    )}
                    {timespan && (
                        <Box sx={{ background: "#F7F5F0", border: "1px solid #D8D2C8", borderRadius: "6px", px: 0.9, py: 0.2 }}>
                            <Typography sx={{ fontSize: "0.57rem", color: "#9C9690", fontFamily: G }}>{timespan.from} → {timespan.to}</Typography>
                        </Box>
                    )}
                </Box>
            </Box>

            <Box sx={{ overflowX: "auto", borderRadius: "12px", border: "1.5px solid #E0D8CC", background: "linear-gradient(175deg,#FDFCFA 0%,#F5F1EB 100%)" }}>
                <svg width={SVG_W} height={totalH} viewBox={`0 0 ${SVG_W} ${totalH}`} style={{ display: "block" }}>
                    <defs>
                        <marker id="pgArrow" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto"><path d="M1,1 L7,4.5 L1,8" fill="none" stroke="#B0A090" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></marker>
                        <marker id="pgArrowRed" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto"><path d="M1,1 L7,4.5 L1,8" fill="none" stroke="#D95F45" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></marker>
                        <pattern id="pgHatch" width="7" height="7" patternTransform="rotate(45)" patternUnits="userSpaceOnUse"><line x1="0" y1="0" x2="0" y2="7" stroke="#D95F45" strokeWidth="0.7" opacity="0.12" /></pattern>
                    </defs>
                    <line x1={nodeCX} y1={PAD_Y + 10} x2={nodeCX} y2={totalH - PAD_Y - 10} stroke="#D8D0C4" strokeWidth="1" strokeDasharray="3,6" />

                    <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 14} textAnchor="middle" fontSize="7.5" fontWeight="700" fill="#B0A090" fontFamily={G} letterSpacing="0.8">METRICS</text>
                    {sideMetrics.map((m, mi) => (
                        <g key={mi}>
                            <rect x={SVG_W - SIDE_W + 4} y={PAD_Y + 26 + mi * 48} width={SIDE_W - 8} height={42} rx="9" fill={m.bg} stroke={m.stroke} strokeWidth="0.8" />
                            <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 44 + mi * 48} textAnchor="middle" fontSize="7" fontWeight="600" fill={m.tc} fontFamily={G} opacity="0.8">{m.label}</text>
                            <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 61 + mi * 48} textAnchor="middle" fontSize="11" fontWeight="800" fill={m.tc} fontFamily={G}>{m.value}</text>
                        </g>
                    ))}

                    {steps.map((step, i) => {
                        const x = PAD_X, y = PAD_Y + i * (NODE_H + ARROW_H), nodeCY = y + NODE_H / 2;
                        const isHot = isBottleneckNode(step, bottleneckActivity);
                        const theme = getNodeTheme(step, isHot);
                        const isHovering = hov === i;
                        const lines = wrapText(step, 18), lineH = 15;
                        const arrowY1 = y + NODE_H, arrowY2 = y + NODE_H + ARROW_H, arrowMid = (arrowY1 + arrowY2) / 2;
                        const nextIsBottleneck = i < steps.length - 1 && isBottleneckNode(steps[i + 1], bottleneckActivity);
                        const showTransLabel = nextIsBottleneck && bottleneckDays != null;

                        const statPills = [];
                        if (i === 0 && totalCases != null) statPills.push({ label: "Cases", value: `${totalCases}`, h: false });
                        if (isHot && bottleneckDays != null) statPills.push({ label: "Avg wait", value: `${bottleneckDays}d`, h: true });
                        if (isHot && bottleneckPct) statPills.push({ label: "Of cycle", value: bottleneckPct, h: false });
                        if (i === steps.length - 1 && excRateRaw) statPills.push({ label: "Exc rate", value: excRateRaw, h: true });
                        if (!isHot && i !== 0 && i !== steps.length - 1 && e2eDays) statPills.push({ label: "E2E", value: `${e2eDays}d`, h: false });

                        return (
                            <g key={i}>
                                {i < steps.length - 1 && (
                                    <g>
                                        <line x1={nodeCX} y1={arrowY1 + 4} x2={nodeCX} y2={arrowY2 - (showTransLabel ? 20 : 14)} stroke={nextIsBottleneck ? "#D95F45" : "#B0A090"} strokeWidth={nextIsBottleneck ? 2.2 : 1.5} strokeDasharray={nextIsBottleneck ? "6,4" : "none"} markerEnd={nextIsBottleneck ? "url(#pgArrowRed)" : "url(#pgArrow)"} />
                                        {showTransLabel && (
                                            <g>
                                                <rect x={nodeCX - 74} y={arrowMid - 12} width={148} height={24} rx="12" fill="#FEE8E0" stroke="#D95F45" strokeWidth="1.2" />
                                                <text x={nodeCX} y={arrowMid + 5} textAnchor="middle" fontSize="9.5" fontWeight="700" fill="#B03030" fontFamily={G}>⏱ {bottleneckDays}d avg · bottleneck ahead</text>
                                            </g>
                                        )}
                                    </g>
                                )}
                                {isHot && <rect x={x} y={y} width={NODE_W} height={NODE_H} rx="12" fill="url(#pgHatch)" />}
                                <rect x={x} y={y} width={NODE_W} height={NODE_H} rx="12" fill={theme.bg} stroke={isHovering ? theme.borderStrong : theme.border} strokeWidth={isHot ? 2.5 : isHovering ? 1.8 : 1.2} onMouseEnter={() => setHov(i)} onMouseLeave={() => setHov(null)} style={{ cursor: "default" }} />
                                {isHot && bottleneckPct && (() => { const pct = Math.min(parseFloat(bottleneckPct) / 100, 1); return (<g><rect x={x + 12} y={y + NODE_H - 6} width={NODE_W - 24} height={4} rx="2" fill={theme.barColor} opacity="0.12" /><rect x={x + 12} y={y + NODE_H - 6} width={(NODE_W - 24) * pct} height={4} rx="2" fill={theme.barColor} opacity="0.6" /></g>); })()}
                                {!isHot && i === steps.length - 1 && excRateNum != null && (<g><rect x={x + 12} y={y + NODE_H - 6} width={NODE_W - 24} height={4} rx="2" fill={theme.barColor} opacity="0.12" /><rect x={x + 12} y={y + NODE_H - 6} width={(NODE_W - 24) * Math.min(excRateNum, 1)} height={4} rx="2" fill={theme.barColor} opacity="0.6" /></g>)}
                                <rect x={x} y={y + 12} width={5} height={NODE_H - 24} rx="2.5" fill={theme.barColor} opacity="0.9" />
                                <circle cx={x + 28} cy={nodeCY} r={14} fill={theme.numBg} />
                                <text x={x + 28} y={nodeCY + 5} textAnchor="middle" fontSize="11" fontWeight="800" fill="#fff" fontFamily={G}>{i + 1}</text>
                                <rect x={x + NODE_W - 80} y={y - 2} width={76} height={18} rx="9" fill={theme.tagBg} stroke={theme.border} strokeWidth="0.8" />
                                <text x={x + NODE_W - 42} y={y + 11} textAnchor="middle" fontSize="7.5" fontWeight="800" fill={theme.tagText} fontFamily={G} letterSpacing="0.4">{isHot ? "🔥 " : ""}{theme.tag}</text>
                                {lines.map((line, li) => (
                                    <text key={li} x={x + 52} y={nodeCY - ((lines.length - 1) * lineH) / 2 + li * lineH - (statPills.length > 0 ? 12 : 4)} fontSize="12.5" fontWeight="700" fill={theme.text} fontFamily={G}>{line}</text>
                                ))}
                                {statPills.map((stat, si) => {
                                    const px = x + 52 + si * 108, py = y + NODE_H - 30;
                                    return (<g key={si}><rect x={px} y={py} width={100} height={19} rx="9.5" fill={stat.h ? theme.badgeBg : "rgba(0,0,0,0.07)"} opacity={stat.h ? 0.88 : 1} /><text x={px + 10} y={py + 13} fontSize="8.5" fontWeight="700" fill={stat.h ? "#fff" : theme.subText} fontFamily={G}>{stat.label}: {stat.value}</text></g>);
                                })}
                            </g>
                        );
                    })}
                </svg>
            </Box>

            <Box sx={{ display: "flex", gap: 0.8, mt: 1, flexWrap: "wrap" }}>
                {avgE2E && <Box sx={{ background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>⏱ {avgE2E}</Typography></Box>}
                {excRateRaw && <Box sx={{ background: "#FAEAEA", border: "1px solid #E0A0A0", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#B03030", fontFamily: G }}>⚠ Exc: {excRateRaw}</Typography></Box>}
                {bottleneckActivity && bottleneckDays != null && <Box sx={{ background: "#FEE8E0", border: "1px solid #F5A58A", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#B03A1A", fontFamily: G }}>🔥 {bottleneckActivity} ({bottleneckDays}d)</Typography></Box>}
                <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", color: "#9C9690", fontFamily: G }}>{steps.length} steps · {pathLabel || "process path"}</Typography></Box>
            </Box>
        </Box>
    );
}

function AgentBadge({ agentUsed }) {
    if (!agentUsed) return null;
    const meta = getAgentMeta(agentUsed);
    return (
        <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: meta.bg, border: `1px solid ${meta.border}`, borderRadius: "6px", px: 0.9, py: 0.25 }}>
            <Typography sx={{ fontSize: "0.6rem", lineHeight: 1 }}>{meta.icon}</Typography>
            <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: meta.color, fontFamily: G }}>{agentUsed}</Typography>
        </Box>
    );
}

function CopyButton({ text }) {
    const [copied, setCopied] = useState(false);
    const handleCopy = () => {
        navigator.clipboard.writeText(text).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        });
    };
    return (
        <Tooltip title={copied ? "Copied!" : "Copy response"}>
            <Box onClick={handleCopy} sx={{ cursor: "pointer", opacity: 0.5, "&:hover": { opacity: 1 }, transition: "opacity 0.15s", display: "flex", alignItems: "center" }}>
                <svg width="13" height="13" viewBox="0 0 16 16" fill="none">
                    {copied
                        ? <path d="M3 8l3 3 7-7" stroke="#43A047" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                        : <><rect x="5" y="5" width="9" height="9" rx="2" stroke="#9C9690" strokeWidth="1.4" /><path d="M3 11V3a2 2 0 012-2h8" stroke="#9C9690" strokeWidth="1.4" strokeLinecap="round" /></>
                    }
                </svg>
            </Box>
        </Tooltip>
    );
}

function ScopeBadge({ scopeLabel }) {
    if (!scopeLabel || scopeLabel === "Global — all cases and vendors") return null;
    const isNotFound = scopeLabel.includes("NOT FOUND");
    return (
        <Box sx={{ background: isNotFound ? "#FAEAEA" : "#F0EDE6", border: `1px solid ${isNotFound ? "#E0A0A0" : "#D8D2C8"}`, borderRadius: "99px", px: 1, py: 0.15, display: "inline-flex", alignItems: "center", gap: 0.5 }}>
            <svg width="8" height="8" viewBox="0 0 9 9" fill="none">
                <circle cx="4.5" cy="4.5" r="3.5" stroke={isNotFound ? "#B03030" : "#B5742A"} strokeWidth="1.2" />
                <circle cx="4.5" cy="4.5" r="1.5" fill={isNotFound ? "#B03030" : "#B5742A"} />
            </svg>
            <Typography sx={{ fontSize: "0.62rem", fontWeight: 600, color: isNotFound ? "#B03030" : "#9C9690", fontFamily: G }}>{scopeLabel}</Typography>
        </Box>
    );
}

// ── FIX 1: Detect cache warming — soft state, not an error ────────────────────
function isCacheWarming(result) {
    return !result.success && result.error === "cache_warming";
}

function UserMessage({ msg }) {
    return (
        <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{ maxWidth: "70%" }}>
                <Box sx={{ background: "#17140F", color: "#F7F5F0", borderRadius: "14px 14px 4px 14px", px: 2, py: 1.2, fontSize: "0.875rem", fontFamily: G, lineHeight: 1.6, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
                    {msg.content}
                </Box>
                <Typography sx={{ fontSize: "0.67rem", color: "#9C9690", fontFamily: G, mt: 0.4, textAlign: "right" }}>{formatTime(msg.ts)}</Typography>
            </Box>
            <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "#EDE9E1", border: "1px solid #D8D2C8", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#5C5650", fontFamily: G }}>ME</Typography>
            </Box>
        </Box>
    );
}

function AssistantMessage({ msg }) {
    const piEvidence = msg.piEvidence || null;
    // FIX 2: Show graph whenever process_path_detected exists in pi_evidence,
    // not just when the user explicitly asked for a graph.
    const showGraph = !!piEvidence?.process_path_detected && !msg.isError && !msg.isWarming;

    // FIX 3: Cache warming is a neutral loading state, not a red error.
    const isWarming = msg.isWarming === true;
    const isError = msg.isError === true && !isWarming;

    const headerBg = isWarming
        ? "rgba(24,95,165,0.07)"
        : isError
            ? "#FAEAEA"
            : showGraph
                ? "rgba(181,116,42,0.07)"
                : "rgba(181,116,42,0.04)";

    const borderLeft = isWarming
        ? "3px solid #185FA5"
        : isError
            ? "3px solid #B03030"
            : "3px solid #B5742A";

    const headerLabel = isWarming
        ? "Connecting to Celonis…"
        : isError
            ? "Error"
            : showGraph
                ? "Process Flow Visualization"
                : "Process Intelligence Finding";

    const headerDotColor = isWarming ? "#185FA5" : isError ? "#B03030" : "#B5742A";
    const headerTextColor = isWarming ? "#185FA5" : isError ? "#B03030" : "#B5742A";

    return (
        <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                <PIIcon size={14} />
            </Box>
            <Box sx={{ maxWidth: "85%", flex: 1 }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 0.6, flexWrap: "wrap" }}>
                    <ScopeBadge scopeLabel={msg.scopeLabel} />
                    {msg.agentUsed && <AgentBadge agentUsed={msg.agentUsed} />}
                </Box>

                <Box sx={{ background: isError ? "#FAEAEA" : "#FFFFFF", border: `1px solid ${isError ? "#E0A0A0" : "#E8E3DA"}`, borderLeft, borderRadius: "4px 10px 10px 10px", overflow: "hidden", boxShadow: "0 1px 4px rgba(23,20,15,0.06)" }}>
                    <Box sx={{ px: 2, py: 0.8, background: headerBg, borderBottom: `1px solid ${isError ? "#E0A0A0" : "#F0EDE6"}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                        <Box sx={{ display: "flex", alignItems: "center", gap: 0.8 }}>
                            <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: headerDotColor, flexShrink: 0 }} />
                            <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: headerTextColor, fontFamily: G, textTransform: "uppercase", letterSpacing: "0.06em" }}>
                                {headerLabel}
                            </Typography>
                        </Box>
                        {!isError && !isWarming && <CopyButton text={msg.content} />}
                    </Box>

                    <Box sx={{ px: 2, py: 1.4, fontSize: "0.85rem", fontFamily: G, lineHeight: 1.7, color: isError ? "#B03030" : isWarming ? "#185FA5" : "#17140F", wordBreak: "break-word", "& p": { margin: 0, marginBottom: "8px", "&:last-child": { marginBottom: 0 } }, "& strong": { fontWeight: 700, color: "#17140F" }, "& ul,& ol": { paddingLeft: "16px", margin: "4px 0" }, "& li": { marginBottom: "3px", lineHeight: 1.6, fontSize: "0.84rem" }, "& code": { fontFamily: "monospace", fontSize: "0.78rem", background: "#F0EDE6", borderRadius: "3px", padding: "1px 4px" } }}>
                        {(isError || isWarming) ? msg.content : <ReactMarkdown>{msg.content}</ReactMarkdown>}
                    </Box>

                    {showGraph && (
                        <Box sx={{ px: 2, pb: 1.5, borderTop: "1px solid #F0EDE6", pt: 1.5 }}>
                            <ProcessGraph piEvidence={piEvidence} pathLabel={msg.pathLabel} />
                        </Box>
                    )}
                </Box>

                {piEvidence?.metrics_used?.length > 0 && !isWarming && (
                    <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 0.8 }}>
                        {piEvidence.metrics_used.slice(0, 4).map((m, i) => {
                            const sc = STATUS_COLORS[m.status] || STATUS_COLORS.normal;
                            return (
                                <Box key={i} sx={{ display: "inline-flex", alignItems: "center", gap: 0.4, background: sc.bg, border: `1px solid ${sc.border}`, borderRadius: "99px", px: 0.9, py: 0.2 }}>
                                    <Box sx={{ width: 4, height: 4, borderRadius: "50%", background: sc.dot, flexShrink: 0 }} />
                                    <Typography sx={{ fontSize: "0.6rem", fontWeight: 600, color: sc.color, fontFamily: G }}>{m.label}: {m.value}</Typography>
                                </Box>
                            );
                        })}
                        {piEvidence.data_completeness && (() => {
                            const cc = CONFIDENCE_COLORS[piEvidence.data_completeness.confidence] || CONFIDENCE_COLORS.medium;
                            return (
                                <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.4, background: cc.bg, border: `1px solid ${cc.border}`, borderRadius: "99px", px: 0.9, py: 0.2 }}>
                                    <Typography sx={{ fontSize: "0.6rem", fontWeight: 600, color: cc.color, fontFamily: G }}>{cc.label}</Typography>
                                </Box>
                            );
                        })()}
                    </Box>
                )}
                <Typography sx={{ fontSize: "0.67rem", color: "#9C9690", fontFamily: G, mt: 0.4 }}>{formatTime(msg.ts)}</Typography>
            </Box>
        </Box>
    );
}

function TypingIndicator() {
    return (
        <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}><PIIcon size={14} /></Box>
            <Box sx={{ background: "#FFFFFF", border: "1px solid #E8E3DA", borderLeft: "3px solid #B5742A", borderRadius: "4px 10px 10px 10px", px: 2, py: 1.2, display: "flex", gap: "5px", alignItems: "center", boxShadow: "0 1px 3px rgba(23,20,15,0.05)" }}>
                <Typography sx={{ fontSize: "0.72rem", color: "#B5742A", fontFamily: G, fontWeight: 600, mr: 1 }}>Analyzing event log</Typography>
                {[0, 0.18, 0.36].map((d, i) => <Box key={i} sx={{ width: 5, height: 5, borderRadius: "50%", background: "#B5742A", animation: "chatBounce 1.2s ease-in-out infinite", animationDelay: `${d}s` }} />)}
            </Box>
            <style>{`@keyframes chatBounce{0%,80%,100%{transform:translateY(0);opacity:.4}40%{transform:translateY(-5px);opacity:1}}`}</style>
        </Box>
    );
}

function EvidenceSection({ label, children }) {
    return (
        <Box sx={{ mb: 1.5 }}>
            <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.6 }}>{label}</Typography>
            {children}
        </Box>
    );
}

// ── FIX 4: PIContextPanel — derive variant path from context_used.case OR
//           top_variants when process_path_detected is absent ─────────────────
function PIContextPanel({ piEvidence, contextUsed, vendorCtx, scopeLabel }) {
    if (!piEvidence) return null;

    // Prefer case-specific detail, then global path, then first top variant
    const caseDetail = piEvidence.case_detail || contextUsed?.case || null;

    const variantPath =
        caseDetail?.variant ||
        piEvidence.process_path_detected ||
        piEvidence.top_variants?.[0]?.path ||
        null;

    const currentStep = caseDetail?.current_stage || null;
    const daysIn = caseDetail?.days_in_process ?? null;

    const globalAvgMetric = (piEvidence.metrics_used || []).find(
        m => m.label === "Avg Cycle Time" || m.label === "Case Cycle Time"
    );
    const globalAvg = globalAvgMetric?.value || null;

    // Golden path for deviation check — use first top_variant path as golden fallback
    const goldenPath =
        piEvidence.process_path_detected ||
        piEvidence.top_variants?.[0]?.path ||
        null;

    let deviationPoint = "None — on golden path";
    let isDeviated = false;
    if (variantPath && goldenPath && variantPath !== goldenPath) {
        const varSteps = variantPath.split(" -> ");
        const goldenSteps = goldenPath.split(" -> ");
        const firstDiff = varSteps.find((s, idx) => s !== goldenSteps[idx]);
        deviationPoint = firstDiff ? `Deviated at: "${firstDiff}"` : "Variant differs from golden path";
        isDeviated = true;
    }

    const vendorHistory = vendorCtx
        ? [
            vendorCtx.exception_rate_pct != null && `${vendorCtx.exception_rate_pct}% exc rate`,
            vendorCtx.total_cases != null && `${vendorCtx.total_cases} cases`,
            vendorCtx.avg_duration_days != null && `avg ${vendorCtx.avg_duration_days}d`,
        ].filter(Boolean).join(" · ") || null
        : null;

    let cycleTimeStr = "[not available]";
    let cycleIsHigh = false;
    if (daysIn != null) {
        const bn = globalAvg ? ` vs benchmark ${globalAvg}` : "";
        cycleTimeStr = `${Number(daysIn).toFixed(2)} days${bn}`;
        if (globalAvg) cycleIsHigh = parseFloat(daysIn) > parseFloat(globalAvg);
    } else if (globalAvg) {
        cycleTimeStr = `Global avg: ${globalAvg}`;
    }

    const displayVariant = variantPath
        ? variantPath.length > 130 ? variantPath.slice(0, 127) + "…" : variantPath
        : "[not available]";

    const timespan = piEvidence.event_log_timespan;
    const sourceStr = timespan
        ? `Celonis Event Log · ${timespan.from} → ${timespan.to}`
        : "Celonis Event Log (live)";

    const rows = [
        { label: "Process step", value: currentStep || "[not available]", highlight: false, accent: !!currentStep },
        { label: "Deviation point", value: deviationPoint, highlight: isDeviated, accent: false },
        { label: "Variant path", value: displayVariant, highlight: false, accent: false, mono: true },
        { label: "Cycle time", value: cycleTimeStr, highlight: cycleIsHigh, accent: false },
        {
            label: "Vendor behavior",
            value: vendorHistory || (scopeLabel?.includes("Vendor") ? "[vendor snapshot unavailable]" : "N/A"),
            highlight: false, accent: false,
        },
        { label: "Source system", value: sourceStr, highlight: false, accent: false, muted: true },
    ];

    return (
        <Card sx={{ mb: 1.5, background: "#FDFCFA !important", borderTop: "2px solid #185FA5 !important" }}>
            <CardContent sx={{ pb: "14px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1.2 }}>
                    <Box sx={{ width: 18, height: 18, borderRadius: "4px", background: "rgba(24,95,165,0.10)", border: "1px solid rgba(24,95,165,0.35)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                        <Typography sx={{ fontSize: "0.52rem", fontWeight: 800, color: "#185FA5", fontFamily: G, letterSpacing: "0.02em" }}>PI</Typography>
                    </Box>
                    <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#185FA5", fontFamily: G, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                        PI Context Used
                    </Typography>
                </Box>
                <Stack spacing={0}>
                    {rows.map((row, i) => (
                        <Box key={i} sx={{ display: "flex", alignItems: "flex-start", gap: 1, py: 0.6, borderBottom: i < rows.length - 1 ? "1px solid #F0EDE6" : "none" }}>
                            <Typography sx={{ fontSize: "0.63rem", color: "#9C9690", fontFamily: G, flexShrink: 0, width: 96, pt: "2px", lineHeight: 1.4 }}>
                                {row.label}
                            </Typography>
                            <Box sx={{ flex: 1 }}>
                                {row.highlight ? (
                                    <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: "#FAEAEA", border: "1px solid #E0A0A0", borderRadius: "6px", px: 0.8, py: 0.2 }}>
                                        <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: "#C94040", flexShrink: 0 }} />
                                        <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color: "#B03030", fontFamily: G, lineHeight: 1.4, wordBreak: "break-word" }}>{row.value}</Typography>
                                    </Box>
                                ) : row.mono ? (
                                    <Typography sx={{ fontSize: "0.63rem", fontFamily: "monospace", color: "#3C2800", lineHeight: 1.5, wordBreak: "break-word", background: "#F5F2EC", border: "1px solid #E8E3DA", borderRadius: "5px", px: 0.7, py: 0.3, display: "block" }}>
                                        {row.value}
                                    </Typography>
                                ) : row.accent ? (
                                    <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.28)", borderRadius: "6px", px: 0.8, py: 0.2 }}>
                                        <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: "#B5742A", flexShrink: 0 }} />
                                        <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color: "#7A4010", fontFamily: G, lineHeight: 1.4 }}>{row.value}</Typography>
                                    </Box>
                                ) : (
                                    <Typography sx={{ fontSize: "0.68rem", fontWeight: row.muted ? 400 : 500, color: row.muted ? "#9C9690" : "#17140F", fontFamily: G, lineHeight: 1.45, wordBreak: "break-word" }}>
                                        {row.value}
                                    </Typography>
                                )}
                            </Box>
                        </Box>
                    ))}
                </Stack>
            </CardContent>
        </Card>
    );
}

function ProcessEvidencePanel({ piEvidence }) {
    if (!piEvidence?.metrics_used) return null;
    const bn = piEvidence.bottleneck_stage, dc = piEvidence.data_completeness, ts = piEvidence.event_log_timespan;
    const cc = dc ? (CONFIDENCE_COLORS[dc.confidence] || CONFIDENCE_COLORS.medium) : null;
    return (
        <Card sx={{ mb: 1.5, background: "#FDFCFA !important", borderTop: "2px solid #B5742A !important" }}>
            <CardContent sx={{ pb: "14px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1.2 }}>
                    <PIIcon size={14} />
                    <Typography sx={{ fontSize: "0.72rem", fontWeight: 700, color: "#B5742A", fontFamily: G, textTransform: "uppercase", letterSpacing: "0.05em" }}>Process Evidence</Typography>
                </Box>
                <EvidenceSection label="Key Metrics">
                    <Stack spacing={0.5}>
                        {piEvidence.metrics_used.map((m, i) => {
                            const sc = STATUS_COLORS[m.status] || STATUS_COLORS.normal;
                            return (
                                <Box key={i} sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", py: 0.4, borderBottom: "1px solid #F0EDE6", "&:last-child": { borderBottom: "none" } }}>
                                    <Typography sx={{ fontSize: "0.7rem", color: "#6C6660", fontFamily: G }}>{m.label}</Typography>
                                    <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                                        <Typography sx={{ fontSize: "0.73rem", fontWeight: 700, color: sc.color, fontFamily: G }}>{m.value}</Typography>
                                        <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: sc.dot }} />
                                    </Box>
                                </Box>
                            );
                        })}
                    </Stack>
                </EvidenceSection>
                {bn && (
                    <EvidenceSection label="Bottleneck Stage">
                        <Box sx={{ background: "#FAEAEA", border: "1px solid #E0A0A0", borderLeft: "3px solid #B03030", borderRadius: "0 6px 6px 0", px: 1.2, py: 0.8 }}>
                            <Typography sx={{ fontSize: "0.78rem", fontWeight: 700, color: "#B03030", fontFamily: G, mb: 0.2 }}>{bn.activity}</Typography>
                            <Typography sx={{ fontSize: "0.68rem", color: "#7C7670", fontFamily: G }}>{bn.avg_days} days avg{bn.pct_of_total ? ` · ${bn.pct_of_total} of cycle` : ""}{bn.case_count ? ` · ${bn.case_count} cases` : ""}</Typography>
                        </Box>
                    </EvidenceSection>
                )}
                {/* FIX 5: Show exception patterns from right panel unconditionally */}
                {piEvidence.exception_patterns?.length > 0 && (
                    <EvidenceSection label="Exception Patterns">
                        <Stack spacing={0.4}>
                            {piEvidence.exception_patterns.map((p, i) => (
                                <Box key={i} sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "6px", px: 1, py: 0.5 }}>
                                    <Typography sx={{ fontSize: "0.67rem", color: "#A05A10", fontFamily: G }}>{p.type}</Typography>
                                    <Typography sx={{ fontSize: "0.67rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>{p.case_count} · {p.frequency_pct}%</Typography>
                                </Box>
                            ))}
                        </Stack>
                    </EvidenceSection>
                )}
                {piEvidence.process_path_detected && (
                    <EvidenceSection label={piEvidence.path_label ? "Process Path" : "Golden Path"}>
                        <Box sx={{ background: "#F0FBF7", border: "1px solid #8FCFC5", borderRadius: "6px", px: 1.2, py: 0.7 }}>
                            {piEvidence.path_label && <Typography sx={{ fontSize: "0.62rem", fontWeight: 700, color: "#1A6B5E", fontFamily: G, mb: 0.3 }}>{piEvidence.path_label}</Typography>}
                            <Typography sx={{ fontSize: "0.68rem", color: "#1A6B5E", fontFamily: G, lineHeight: 1.55, wordBreak: "break-word" }}>{piEvidence.process_path_detected}</Typography>
                            {piEvidence.golden_path_percentage != null && <Typography sx={{ fontSize: "0.62rem", color: "#9C9690", fontFamily: G, mt: 0.3 }}>{piEvidence.golden_path_percentage}% follow golden path</Typography>}
                        </Box>
                    </EvidenceSection>
                )}
                {dc && cc && (
                    <EvidenceSection label="Data Quality">
                        <Box sx={{ background: cc.bg, border: `1px solid ${cc.border}`, borderRadius: "6px", px: 1.2, py: 0.7 }}>
                            <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 0.3 }}>
                                <Typography sx={{ fontSize: "0.68rem", fontWeight: 600, color: cc.color, fontFamily: G }}>{cc.label}</Typography>
                                <Typography sx={{ fontSize: "0.62rem", color: "#6C6660", fontFamily: G }}>{dc.cases_analyzed} cases · {dc.events_analyzed} events</Typography>
                            </Box>
                            <Typography sx={{ fontSize: "0.62rem", color: "#6C6660", fontFamily: G }}>{dc.note}</Typography>
                            {ts && <Typography sx={{ fontSize: "0.6rem", color: "#9C9690", fontFamily: G, mt: 0.3 }}>Log: {ts.from} → {ts.to}</Typography>}
                        </Box>
                    </EvidenceSection>
                )}
            </CardContent>
        </Card>
    );
}

function SimilarCasesPanel({ similarCases }) {
    if (!similarCases?.length) return null;
    return (
        <Card sx={{ mb: 1.5 }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1 }}>
                    <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G }}>Similar Cases</Typography>
                    <Box sx={{ background: "#17140F", borderRadius: "99px", px: 0.8, py: 0.1 }}><Typography sx={{ fontSize: "0.58rem", fontWeight: 700, color: "#F7F5F0", fontFamily: G }}>{similarCases.length}</Typography></Box>
                </Box>
                <Stack spacing={0.5}>
                    {similarCases.map(sc => (
                        <Box key={sc.case_id} sx={{ background: "#FAFAF8", border: "1px solid #E8E3DA", borderRadius: "6px", px: 1.2, py: 0.7, "&:hover": { borderColor: "#B5742A" }, transition: "border-color 0.15s" }}>
                            <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                <Typography sx={{ fontSize: "0.72rem", fontWeight: 600, color: "#17140F", fontFamily: G }}>{sc.case_id}</Typography>
                                {sc.duration_days != null && <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G }}>{sc.duration_days}d</Typography>}
                            </Box>
                            {sc.current_stage && <Typography sx={{ fontSize: "0.65rem", color: "#5C5650", fontFamily: G, mt: 0.2 }}>{sc.current_stage}</Typography>}
                        </Box>
                    ))}
                </Stack>
            </CardContent>
        </Card>
    );
}

function VendorPanel({ vendorData }) {
    if (!vendorData?.total_cases) return null;
    const excRate = parseFloat(vendorData.exception_rate_pct ?? 0), overallExc = parseFloat(vendorData.overall_exception_rate_pct ?? 0);
    const avgDur = parseFloat(vendorData.avg_duration_days ?? 0), overallDur = parseFloat(vendorData.overall_avg_duration_days ?? 0);
    const { label: riskLabel, color: riskColor, bg: riskBg, border: riskBorder } = getRisk(excRate, overallExc, avgDur, overallDur);
    return (
        <Card sx={{ mb: 1.5, background: `${riskBg} !important`, border: `1.5px solid ${riskBorder} !important` }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", mb: 1 }}>
                    <Box>
                        <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G }}>Vendor Intelligence</Typography>
                        <Typography sx={{ fontSize: "0.85rem", fontWeight: 700, color: "#17140F", fontFamily: G, mt: 0.2 }}>{vendorData.vendor_id || "—"}</Typography>
                    </Box>
                    <Box sx={{ background: riskColor, borderRadius: "5px", px: 0.8, py: 0.2 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 800, color: "#fff", fontFamily: G }}>{riskLabel}</Typography></Box>
                </Box>
                <Box sx={{ background: "rgba(255,255,255,0.7)", borderRadius: "6px", px: 1, py: 0.3, mb: 0.8 }}>
                    {[["Total invoices", vendorData.total_cases], ["Exception rate", `${excRate}%`, excRate > overallExc + 5], ["Avg cycle time", `${avgDur} days`, avgDur > overallDur + 2], ["Payment terms", vendorData.payment_terms || "—"]].map(([label, value, highlight]) => (
                        <Box key={label} sx={{ display: "flex", justifyContent: "space-between", py: 0.35, borderBottom: "1px solid rgba(0,0,0,0.05)", "&:last-child": { borderBottom: "none" } }}>
                            <Typography sx={{ fontSize: "0.67rem", color: "#6B6560", fontFamily: G }}>{label}</Typography>
                            <Typography sx={{ fontSize: "0.7rem", fontWeight: 700, color: highlight ? riskColor : "#17140F", fontFamily: G }}>{value ?? "—"}</Typography>
                        </Box>
                    ))}
                </Box>
                <Box sx={{ display: "flex", gap: 0.6 }}>
                    <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 0.8, py: 0.5, textAlign: "center" }}>
                        <Typography sx={{ fontSize: "0.55rem", color: "#9C9690", fontFamily: G }}>vs avg exception</Typography>
                        <Typography sx={{ fontSize: "0.78rem", fontWeight: 800, color: riskColor, fontFamily: G }}>{(excRate - overallExc) > 0 ? `+${(excRate - overallExc).toFixed(1)}` : (excRate - overallExc).toFixed(1)}%</Typography>
                    </Box>
                    <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 0.8, py: 0.5, textAlign: "center" }}>
                        <Typography sx={{ fontSize: "0.55rem", color: "#9C9690", fontFamily: G }}>vs avg cycle</Typography>
                        <Typography sx={{ fontSize: "0.78rem", fontWeight: 800, color: riskColor, fontFamily: G }}>{(avgDur - overallDur) > 0 ? `+${(avgDur - overallDur).toFixed(1)}` : (avgDur - overallDur).toFixed(1)}d</Typography>
                    </Box>
                </Box>
                {vendorData.top_exception_types?.length > 0 && (
                    <Box sx={{ mt: 0.8 }}>
                        <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Exception types (this vendor)</Typography>
                        <Stack spacing={0.3}>
                            {vendorData.top_exception_types.map((e, i) => (
                                <Box key={i} sx={{ display: "flex", justifyContent: "space-between", background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "5px", px: 0.8, py: 0.3 }}>
                                    <Typography sx={{ fontSize: "0.64rem", color: "#3C3830", fontFamily: G }}>{e.exception_type}</Typography>
                                    <Typography sx={{ fontSize: "0.64rem", fontWeight: 700, color: riskColor, fontFamily: G }}>{e.case_count} {e.pct ? `(${e.pct}%)` : ""}</Typography>
                                </Box>
                            ))}
                        </Stack>
                    </Box>
                )}
            </CardContent>
        </Card>
    );
}

// FIX 6: DataSourcesPanel reads from per-message dataSources, not global state
function DataSourcesPanel({ dataSources }) {
    const [open, setOpen] = useState(false);
    if (!dataSources?.length) return null;
    return (
        <Card sx={{ mb: 1.5 }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <Box onClick={() => setOpen(o => !o)} sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 0.8 }}>
                        <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G }}>Source Data</Typography>
                        <Box sx={{ background: "#17140F", borderRadius: "99px", px: 0.8, py: 0.1 }}><Typography sx={{ fontSize: "0.58rem", fontWeight: 700, color: "#F7F5F0", fontFamily: G }}>{dataSources.length}</Typography></Box>
                    </Box>
                    <Typography sx={{ fontSize: "0.65rem", color: "#B5742A", fontFamily: G }}>{open ? "Hide ▲" : "Show ▼"}</Typography>
                </Box>
                <Collapse in={open}>
                    <Stack spacing={0.4} sx={{ mt: 0.8 }}>
                        {dataSources.map((src, i) => (
                            <Box key={i} sx={{ background: "#F7F5F0", border: "1px solid #E8E3DA", borderRadius: "5px", px: 1, py: 0.5 }}>
                                <Typography sx={{ fontSize: "0.65rem", color: "#5C5650", fontFamily: G }}>{src}</Typography>
                            </Box>
                        ))}
                    </Stack>
                    <Box sx={{ mt: 0.6, px: 0.8, py: 0.3, background: "#F0FBF7", borderRadius: "4px", border: "1px solid #8FCFC5" }}>
                        <Typography sx={{ fontSize: "0.58rem", color: "#1A6B5E", fontFamily: G }}>Live from Celonis event log — not cached or static</Typography>
                    </Box>
                </Collapse>
            </CardContent>
        </Card>
    );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function PIChat() {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState("");
    const [isTyping, setIsTyping] = useState(false);
    const [suggestedQs, setSuggestedQs] = useState([]);
    const [error, setError] = useState(null);
    const messagesEndRef = useRef(null);
    const textareaRef = useRef(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages, isTyping]);

    useEffect(() => {
        const ta = textareaRef.current;
        if (!ta) return;
        ta.style.height = "auto";
        ta.style.height = Math.min(ta.scrollHeight, 120) + "px";
    }, [input]);

    // FIX 7: All right-panel data comes from the latest assistant message,
    // never from a separate top-level state that can go stale.
    const latestAssistant = useMemo(() => {
        for (let i = messages.length - 1; i >= 0; i--) {
            if (messages[i].role === "assistant" && !messages[i].isError && !messages[i].isWarming) {
                return messages[i];
            }
        }
        return null;
    }, [messages]);

    const piEvidence = latestAssistant?.piEvidence || null;
    const similarCases = latestAssistant?.similarCases || null;
    const vendorContext = latestAssistant?.vendorContext || null;
    const contextUsed = latestAssistant?.context_used || null;
    // FIX 6 continued: pull dataSources from the message, not top-level state
    const dataSources = latestAssistant?.dataSources || [];

    const hasReplied = messages.some(m => m.role === "assistant" && !m.isError && !m.isWarming);
    const showRight = piEvidence || similarCases || vendorContext || dataSources.length > 0 || hasReplied;

    const handleSend = useCallback(async (text) => {
        const msg = (text || input).trim();
        if (!msg || isTyping) return;

        setInput("");
        setError(null);
        setSuggestedQs([]);

        setMessages(prev => [...prev, { id: Date.now(), role: "user", content: msg, ts: new Date() }]);
        setIsTyping(true);

        try {
            const history = messages.slice(-8).map(m => ({ role: m.role, content: m.content }));
            const result = await sendChatMessage({ message: msg, conversationHistory: history });

            // FIX 1 & 3: Check for cache warming before treating as error
            const warming = isCacheWarming(result);

            setMessages(prev => [...prev, {
                id: Date.now() + 1,
                role: "assistant",
                content: result.reply || "No response received.",
                ts: new Date(),
                isError: !result.success && !warming,
                isWarming: warming,
                scopeLabel: result.scope_label || "",
                agentUsed: result.agent_used || "",
                // FIX 2: showGraph driven by pi_evidence.process_path_detected, set here for clarity
                showGraph: !!result.pi_evidence?.process_path_detected,
                pathLabel: result.path_label || "",
                piEvidence: result.pi_evidence || null,
                similarCases: result.similar_cases || null,
                vendorContext: result.vendor_context || null,
                context_used: result.context_used || null,
                // FIX 6: store dataSources on the message itself
                dataSources: result.data_sources || [],
            }]);

            if (result.suggested_questions) setSuggestedQs(result.suggested_questions);

        } catch (err) {
            setMessages(prev => [...prev, {
                id: Date.now() + 1,
                role: "assistant",
                content: `Unable to reach the PI backend: ${err.message}`,
                ts: new Date(),
                isError: true,
            }]);
            setError(err.message);
        } finally {
            setIsTyping(false);
        }
    }, [input, isTyping, messages]);

    const handleKeyDown = (e) => {
        if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
    };

    const STARTER_QUESTIONS = [
        "Where is the biggest bottleneck in the process?",
        "Which vendors have the highest exception rates?",
        "Show me all active exceptions right now",
        "Show me the process flow for delayed invoices",
        "What is the overall exception rate vs target?",
        "Draw the exception path in the process",
        "What does vendor 7003196321 look like?",
    ];

    return (
        <div className="page-container">
            <Box sx={{ pt: 4, pb: 2.5, borderBottom: "1px solid #E8E3DA", mb: 2.5 }}>
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                    <Box>
                        <Typography sx={{ fontFamily: S, fontSize: "2.1rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.4 }}>Process Intelligence Console</Typography>
                        <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>
                            Live Celonis data · Vendor IDs detected automatically · Type any vendor or case ID in plain language
                        </Typography>
                    </Box>
                    {messages.length > 0 && (
                        <Button variant="outlined" size="small"
                            onClick={() => { setMessages([]); setSuggestedQs([]); setError(null); }}
                            sx={{ fontFamily: G, fontSize: "0.78rem", textTransform: "none", borderRadius: "8px" }}>
                            New Investigation
                        </Button>
                    )}
                </Box>
            </Box>

            <Grid container spacing={2.5} sx={{ height: "calc(100vh - 210px)", minHeight: 500 }}>
                <Grid item xs={12} md={7} sx={{ display: "flex", flexDirection: "column" }}>
                    <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                        <Box sx={{ flex: 1, overflow: "auto", px: 2.5, py: 2.5, display: "flex", flexDirection: "column", gap: 2 }}>
                            {messages.length === 0 ? (
                                <Box sx={{ margin: "auto", textAlign: "center", maxWidth: 440, py: 4 }}>
                                    <Box sx={{ width: 52, height: 52, borderRadius: "14px", background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.25)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}><PIIcon size={24} /></Box>
                                    <Typography sx={{ fontFamily: S, fontSize: "1.35rem", color: "#17140F", mb: 0.6 }}>Process Intelligence Console</Typography>
                                    <Typography sx={{ fontSize: "0.8rem", color: "#9C9690", fontFamily: G, lineHeight: 1.65, mb: 2.5 }}>
                                        Every answer is grounded in your Celonis event log. Just type — vendor IDs, case IDs, and questions all work in plain English.
                                    </Typography>
                                    <Box sx={{ display: "flex", flexDirection: "column", gap: 0.6, alignItems: "center" }}>
                                        <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.4 }}>Start an Investigation</Typography>
                                        {STARTER_QUESTIONS.map(q => (
                                            <Box key={q} onClick={() => handleSend(q)} sx={{ background: "#FAFAF8", border: "1px solid #E8E3DA", borderRadius: "8px", px: 1.6, py: 0.7, cursor: "pointer", transition: "all 0.15s", width: "100%", "&:hover": { borderColor: "#B5742A", background: "rgba(181,116,42,0.04)" } }}>
                                                <Typography sx={{ fontSize: "0.78rem", color: "#5C5650", fontFamily: G, textAlign: "left" }}>{q}</Typography>
                                            </Box>
                                        ))}
                                    </Box>
                                    <Box sx={{ display: "flex", justifyContent: "center", gap: 0.8, flexWrap: "wrap", mt: 2 }}>
                                        {["Event log mining", "Variant analysis", "Conformance checking", "Bottleneck detection"].map(t => (
                                            <Box key={t} sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1, py: 0.3 }}><Typography sx={{ fontSize: "0.65rem", color: "#5C5650", fontFamily: G }}>{t}</Typography></Box>
                                        ))}
                                    </Box>
                                </Box>
                            ) : (
                                messages.map(msg =>
                                    msg.role === "user"
                                        ? <UserMessage key={msg.id} msg={msg} />
                                        : <AssistantMessage key={msg.id} msg={msg} />
                                )
                            )}
                            {isTyping && <TypingIndicator />}
                            <div ref={messagesEndRef} />
                        </Box>

                        <Box sx={{ borderTop: "1px solid #E8E3DA", p: 2 }}>
                            {suggestedQs.length > 0 && !isTyping && (
                                <Box sx={{ mb: 1.5 }}>
                                    <Typography sx={{ fontSize: "0.63rem", color: "#9C9690", fontFamily: G, mb: 0.6, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em" }}>Investigate Further</Typography>
                                    <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
                                        {suggestedQs.map(q => (
                                            <Box key={q} onClick={() => handleSend(q)} sx={{ background: "rgba(181,116,42,0.06)", border: "1px solid rgba(181,116,42,0.2)", borderRadius: "99px", px: 1.2, py: 0.35, fontSize: "0.72rem", fontFamily: G, color: "#B5742A", cursor: "pointer", transition: "all 0.15s", "&:hover": { background: "rgba(181,116,42,0.12)", borderColor: "rgba(181,116,42,0.4)" } }}>
                                                {q}
                                            </Box>
                                        ))}
                                    </Box>
                                </Box>
                            )}
                            {error && (
                                <Alert severity="error" sx={{ mb: 1.5, fontSize: "0.78rem" }} onClose={() => setError(null)}>{error}</Alert>
                            )}
                            <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-end", background: "#F7F5F0", border: "1px solid #D8D2C8", borderRadius: "12px", px: 2, py: 1.2, transition: "border-color 0.15s", "&:focus-within": { borderColor: "#B5742A" } }}>
                                <Box
                                    ref={textareaRef}
                                    component="textarea"
                                    value={input}
                                    onChange={e => setInput(e.target.value)}
                                    onKeyDown={handleKeyDown}
                                    disabled={isTyping}
                                    placeholder="Ask about bottlenecks, exceptions, vendors… or type a vendor ID like 7003196321"
                                    rows={1}
                                    sx={{ flex: 1, background: "transparent", border: "none", outline: "none", resize: "none", fontFamily: G, fontSize: "0.875rem", color: "#17140F", lineHeight: 1.55, minHeight: "22px", maxHeight: "120px", overflow: "auto", "::placeholder": { color: "#9C9690" }, "&:disabled": { opacity: 0.5 } }}
                                />
                                <Button variant="contained" size="small" onClick={() => handleSend()} disabled={!input.trim() || isTyping}
                                    sx={{ flexShrink: 0, minWidth: 0, px: 1.5, py: 0.8, borderRadius: "8px", alignSelf: "flex-end" }}>
                                    {isTyping
                                        ? <CircularProgress size={14} sx={{ color: "#FFFFFF" }} />
                                        : <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8h12M10 4l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
                                    }
                                </Button>
                            </Box>
                            <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G, mt: 0.6, textAlign: "center" }}>
                                Connected to Celonis · Vendor IDs auto-detected · Shift+Enter for new line
                            </Typography>
                        </Box>
                    </Card>
                </Grid>

                <Grid item xs={12} md={5} sx={{ display: "flex", flexDirection: "column" }}>
                    <Box sx={{ height: "100%", overflow: "auto", display: "flex", flexDirection: "column" }}>
                        {!showRight ? (
                            <Card sx={{ flex: 1 }}>
                                <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", opacity: 0.5 }}>
                                    <Box sx={{ width: 40, height: 40, borderRadius: "10px", background: "#F0EDE6", border: "1px solid #E8E3DA", display: "flex", alignItems: "center", justifyContent: "center", mb: 1.5 }}><PIIcon size={18} /></Box>
                                    <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, textAlign: "center", maxWidth: 200, lineHeight: 1.6 }}>Process evidence, similar cases, vendor intelligence, and data sources appear here after your first question</Typography>
                                </CardContent>
                            </Card>
                        ) : (
                            <>
                                <PIContextPanel
                                    piEvidence={piEvidence}
                                    contextUsed={contextUsed}
                                    vendorCtx={vendorContext}
                                    scopeLabel={latestAssistant?.scopeLabel || ""}
                                />
                                <ProcessEvidencePanel piEvidence={piEvidence} />
                                <SimilarCasesPanel similarCases={similarCases} />
                                <VendorPanel vendorData={vendorContext} />
                                {/* FIX 6: pass dataSources from latestAssistant message */}
                                <DataSourcesPanel dataSources={dataSources} />
                            </>
                        )}
                    </Box>
                </Grid>
            </Grid>
        </div>
    );
}