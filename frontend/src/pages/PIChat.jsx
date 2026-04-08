import React, { useState, useEffect, useRef, useMemo } from "react";
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
import ReactMarkdown from "react-markdown";
import { sendChatMessage } from "../api/client";

const S = "'Instrument Serif', Georgia, serif";
const G = "'Geist', system-ui, sans-serif";

// ── Agent config ──────────────────────────────────────────────────────────────

const AGENT_META = {
    "Process Intelligence Agent": { icon: "⬡", color: "#B5742A", bg: "rgba(181,116,42,0.10)", border: "rgba(181,116,42,0.30)" },
    "Process Insight Agent": { icon: "⏱", color: "#185FA5", bg: "rgba(24,95,165,0.09)", border: "rgba(24,95,165,0.28)" },
    "Exception Detection Agent": { icon: "⚠", color: "#B03030", bg: "rgba(176,48,48,0.09)", border: "rgba(176,48,48,0.28)" },
    "Vendor Intelligence Agent": { icon: "🏭", color: "#1A6B5E", bg: "rgba(26,107,94,0.09)", border: "rgba(26,107,94,0.28)" },
    "Conformance Checker Agent": { icon: "✓", color: "#6B3FA0", bg: "rgba(107,63,160,0.09)", border: "rgba(107,63,160,0.28)" },
    "Invoice Processing Agent": { icon: "📄", color: "#555", bg: "rgba(85,85,85,0.09)", border: "rgba(85,85,85,0.28)" },
    "Case Resolution Agent": { icon: "🔁", color: "#A05A10", bg: "rgba(160,90,16,0.09)", border: "rgba(160,90,16,0.28)" },
};

function getAgentMeta(name) {
    return AGENT_META[name] || AGENT_META["Process Intelligence Agent"];
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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
    const isHigh = !isCritical && (excRate >= 40 || (avgDur > overallDur + 5));
    const isMedium = !isCritical && !isHigh && (excRate >= 20 || (avgDur > overallDur + 2));
    const label = isCritical ? "CRITICAL" : isHigh ? "HIGH" : isMedium ? "MEDIUM" : "LOW";
    const color = isCritical ? "#B03A1A" : isHigh ? "#B5742A" : isMedium ? "#185FA5" : "#1A6B5E";
    const bg = isCritical ? "#FEE8E0" : isHigh ? "#FEF3DC" : isMedium ? "#EBF4FD" : "#F2FAF6";
    const border = isCritical ? "#F5A58A" : isHigh ? "#F0C870" : isMedium ? "#B5D4F4" : "#B8DFD0";
    return { label, color, bg, border };
}

// ── Reply parser — extracts structured PI Context Used fields from LLM reply ──
//
// The LLM is instructed to output:
//
//   **PI Context Used:**
//   - Process step: ...
//   - Deviation point: ...
//   - Variant path: ...
//   - Cycle time: ...
//   - Vendor behavior: ...
//
// This function extracts those values robustly regardless of whitespace,
// bold markers, or minor formatting variations.

function parsePIContext(replyText) {
    if (!replyText) return null;

    // Find the PI Context Used block
    const blockMatch = replyText.match(
        /\*{0,2}PI Context Used\*{0,2}[\s\S]*?(?=\n\n\*{0,2}[A-Z]|\n\n#{1,3} |\n---|\Z|$)/i
    );
    const block = blockMatch ? blockMatch[0] : replyText;

    function extractField(fieldNames) {
        for (const name of fieldNames) {
            // Match "- Field name: value" or "* Field name: value" or "**Field name:** value"
            const regex = new RegExp(
                `[-*•]?\\s*\\*{0,2}${name}\\*{0,2}\\s*:\\s*([^\\n]+)`,
                "i"
            );
            const m = block.match(regex);
            if (m) {
                const val = m[1]
                    .replace(/\*\*/g, "")   // strip bold markers
                    .replace(/`/g, "")       // strip code ticks
                    .trim();
                if (val && val !== "N/A" && val !== "n/a" && val.toLowerCase() !== "none") {
                    return val;
                }
            }
        }
        return null;
    }

    const processStep = extractField(["Process step", "Process Step", "Current step", "Activity"]);
    const deviationPoint = extractField(["Deviation point", "Deviation Point", "Deviation"]);
    const variantPath = extractField(["Variant path", "Variant Path", "Path", "Process path"]);
    const cycleTime = extractField(["Cycle time", "Cycle Time"]);
    const vendorBehavior = extractField(["Vendor behavior", "Vendor Behavior", "Vendor"]);
    const sourceSystem = extractField(["Source system", "Source System", "Source", "Data source"]);

    // Return null if nothing useful was parsed
    if (!processStep && !variantPath && !cycleTime) return null;

    return { processStep, deviationPoint, variantPath, cycleTime, vendorBehavior, sourceSystem };
}

// ── PI Context Used panel (right sidebar) ─────────────────────────────────────

function PIContextPanel({ replyText, piEvidence }) {
    const ctx = useMemo(() => parsePIContext(replyText), [replyText]);
    if (!ctx) return null;

    const ts = piEvidence?.event_log_timespan;
    const sourceLabel = ctx.sourceSystem
        || (ts ? `Celonis Event Log · ${ts.from} → ${ts.to}` : "Celonis Event Log");

    const rows = [
        { label: "Process step", value: ctx.processStep },
        { label: "Deviation point", value: ctx.deviationPoint },
        { label: "Variant path", value: ctx.variantPath },
        { label: "Cycle time", value: ctx.cycleTime },
        { label: "Vendor behavior", value: ctx.vendorBehavior },
        { label: "Source system", value: sourceLabel },
    ].filter(r => r.value);

    if (rows.length === 0) return null;

    return (
        <Card sx={{ mb: 1.5, background: "#F8F6FF !important", borderTop: "2px solid #185FA5 !important" }}>
            <CardContent sx={{ pb: "14px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1.2 }}>
                    <Box sx={{ width: 8, height: 8, borderRadius: "50%", background: "#185FA5" }} />
                    <Typography sx={{ fontSize: "0.68rem", fontWeight: 700, color: "#185FA5", fontFamily: G, textTransform: "uppercase", letterSpacing: "0.06em" }}>
                        PI Context Used
                    </Typography>
                </Box>
                <Stack spacing={0}>
                    {rows.map(({ label, value }) => (
                        <Box key={label} sx={{ display: "flex", gap: 1, py: 0.55, borderBottom: "1px solid #EDE8F8", "&:last-child": { borderBottom: "none" } }}>
                            <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G, minWidth: 108, flexShrink: 0 }}>
                                {label}
                            </Typography>
                            <Typography sx={{ fontSize: "0.68rem", fontWeight: 600, color: "#17140F", fontFamily: G, wordBreak: "break-word", lineHeight: 1.5 }}>
                                {value}
                            </Typography>
                        </Box>
                    ))}
                </Stack>
            </CardContent>
        </Card>
    );
}

// ── Process Graph ─────────────────────────────────────────────────────────────

function parseProcessPath(piEvidence) {
    const raw = piEvidence?.process_path_detected || "";
    if (!raw) return [];
    return raw.split("→").map(s => s.trim()).filter(Boolean);
}

function isBottleneckNode(stepLabel, bottleneckActivity) {
    if (!bottleneckActivity) return false;
    return stepLabel.toLowerCase().includes(bottleneckActivity.toLowerCase());
}

function getNodeTheme(label, isBottleneck) {
    const l = label.toLowerCase();
    if (isBottleneck) return {
        bg: "#FFF0ED", border: "#D95F45", borderStrong: "#C04A32",
        badgeBg: "#D95F45", text: "#5C1A00", subText: "#B03030",
        barColor: "#D95F45", numBg: "#D95F45",
        tag: "BOTTLENECK", tagBg: "#D95F45", tagText: "#fff",
    };
    if (l.includes("exception") || l.includes("block") || l.includes("due date")) return {
        bg: "#FFF5F5", border: "#F0A0A0", borderStrong: "#D95F45",
        badgeBg: "#E05050", text: "#6B1A1A", subText: "#B03030",
        barColor: "#E05050", numBg: "#E05050",
        tag: "EXCEPTION", tagBg: "#FEE8E0", tagText: "#C04040",
    };
    if (l.includes("received") || l.includes("start") || l.includes("creat")) return {
        bg: "#F2FBF5", border: "#7ACC90", borderStrong: "#43A047",
        badgeBg: "#43A047", text: "#1B4A22", subText: "#2E7D32",
        barColor: "#43A047", numBg: "#43A047",
        tag: "START", tagBg: "#E8F5E9", tagText: "#2E7D32",
    };
    if (l.includes("moved") || l.includes("end") || l.includes("complet") || l.includes("post") || l.includes("paid")) return {
        bg: "#F0F7FF", border: "#90C4F0", borderStrong: "#1E88E5",
        badgeBg: "#1E88E5", text: "#0D3060", subText: "#1565C0",
        barColor: "#1E88E5", numBg: "#1E88E5",
        tag: "END", tagBg: "#E3F2FD", tagText: "#1565C0",
    };
    return {
        bg: "#FAFAF7", border: "#C8C0B0", borderStrong: "#A08060",
        badgeBg: "#A08060", text: "#3C2800", subText: "#6C5840",
        barColor: "#A08060", numBg: "#A08060",
        tag: "STEP", tagBg: "#F0EDE6", tagText: "#7C6850",
    };
}

function wrapText(label, maxChars = 18) {
    const words = label.split(" ");
    const lines = [];
    let cur = "";
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
    const avgE2E = piEvidence?.metrics_used?.find(m => m.label === "Avg Cycle Time")?.value || null;
    const excRateRaw = piEvidence?.metrics_used?.find(m => m.label === "Exception Rate")?.value || null;
    const goldenPct = piEvidence?.golden_path_percentage ?? null;
    const totalCases = piEvidence?.data_completeness?.cases_analyzed ?? null;
    const confidence = piEvidence?.data_completeness?.confidence ?? null;
    const timespan = piEvidence?.event_log_timespan ?? null;
    const e2eDays = avgE2E ? parseFloat(avgE2E) : null;
    const excRateNum = excRateRaw ? parseFloat(excRateRaw) / 100 : null;

    const NODE_W = 310;
    const NODE_H = 96;
    const ARROW_H = 60;
    const PAD_X = 20;
    const PAD_Y = 28;
    const SIDE_W = 80;
    const SVG_W = NODE_W + PAD_X * 2 + SIDE_W;
    const totalH = PAD_Y * 2 + steps.length * NODE_H + (steps.length - 1) * ARROW_H;
    const nodeCX = PAD_X + NODE_W / 2;

    function getNodeStats(step, i) {
        const isFirst = i === 0;
        const isLast = i === steps.length - 1;
        const isHot = isBottleneckNode(step, bottleneckActivity);
        const stats = [];
        if (isFirst && totalCases != null)
            stats.push({ label: "Cases in", value: `${totalCases}`, highlight: false });
        if (isHot && bottleneckDays != null)
            stats.push({ label: "Avg wait", value: `${bottleneckDays}d`, highlight: true });
        if (isHot && bottleneckPct)
            stats.push({ label: "Of cycle", value: bottleneckPct, highlight: false });
        if (isLast && excRateRaw)
            stats.push({ label: "Exc rate", value: excRateRaw, highlight: true });
        if (!isHot && !isFirst && !isLast && e2eDays)
            stats.push({ label: "E2E avg", value: `${e2eDays}d`, highlight: false });
        return stats;
    }

    const confColor = confidence === "high"
        ? { bg: "#E8F5E9", border: "#A5D6A7", text: "#2E7D32" }
        : confidence === "medium"
            ? { bg: "#FEF3DC", border: "#F0C870", text: "#A05A10" }
            : { bg: "#FAEAEA", border: "#E0A0A0", text: "#B03030" };

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
                            <Typography sx={{ fontSize: "0.58rem", fontWeight: 700, color: confColor.text, fontFamily: G }}>
                                {confidence.toUpperCase()} CONFIDENCE
                            </Typography>
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
                        <marker id="pgArrow" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto">
                            <path d="M1,1 L7,4.5 L1,8" fill="none" stroke="#B0A090" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                        </marker>
                        <marker id="pgArrowRed" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto">
                            <path d="M1,1 L7,4.5 L1,8" fill="none" stroke="#D95F45" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                        </marker>
                        <pattern id="pgHatch" width="7" height="7" patternTransform="rotate(45)" patternUnits="userSpaceOnUse">
                            <line x1="0" y1="0" x2="0" y2="7" stroke="#D95F45" strokeWidth="0.7" opacity="0.12" />
                        </pattern>
                    </defs>
                    <line x1={nodeCX} y1={PAD_Y + 10} x2={nodeCX} y2={totalH - PAD_Y - 10} stroke="#D8D0C4" strokeWidth="1" strokeDasharray="3,6" />
                    <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 14} textAnchor="middle" fontSize="7.5" fontWeight="700" fill="#B0A090" fontFamily={G} letterSpacing="0.8">METRICS</text>
                    {[
                        avgE2E && { label: "E2E", value: avgE2E, bg: "#FEF3DC", stroke: "#F0C870", textColor: "#7A4010" },
                        excRateRaw && { label: "EXC", value: excRateRaw, bg: "#FAEAEA", stroke: "#E0A0A0", textColor: "#8B1A1A" },
                        bottleneckDays != null && { label: "BN", value: `${bottleneckDays}d`, bg: "#FEE8E0", stroke: "#F5A58A", textColor: "#8B2010" },
                        totalCases != null && { label: "CASES", value: `${totalCases}`, bg: "#F0EDE6", stroke: "#D8D2C8", textColor: "#3C2800" },
                    ].filter(Boolean).map((m, mi) => (
                        <g key={mi}>
                            <rect x={SVG_W - SIDE_W + 4} y={PAD_Y + 26 + mi * 48} width={SIDE_W - 8} height={42} rx="9" fill={m.bg} stroke={m.stroke} strokeWidth="0.8" />
                            <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 44 + mi * 48} textAnchor="middle" fontSize="7" fontWeight="600" fill={m.textColor} fontFamily={G} opacity="0.8">{m.label}</text>
                            <text x={SVG_W - SIDE_W / 2} y={PAD_Y + 61 + mi * 48} textAnchor="middle" fontSize="11" fontWeight="800" fill={m.textColor} fontFamily={G}>{m.value}</text>
                        </g>
                    ))}
                    {steps.map((step, i) => {
                        const x = PAD_X;
                        const y = PAD_Y + i * (NODE_H + ARROW_H);
                        const nodeCY = y + NODE_H / 2;
                        const isHot = isBottleneckNode(step, bottleneckActivity);
                        const theme = getNodeTheme(step, isHot);
                        const isHovering = hov === i;
                        const lines = wrapText(step, 18);
                        const lineH = 15;
                        const nodeStats = getNodeStats(step, i);
                        const arrowY1 = y + NODE_H;
                        const arrowY2 = y + NODE_H + ARROW_H;
                        const arrowMid = (arrowY1 + arrowY2) / 2;
                        const nextIsBottleneck = i < steps.length - 1 && isBottleneckNode(steps[i + 1], bottleneckActivity);
                        const showTransLabel = nextIsBottleneck && bottleneckDays != null;
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
                                {isHot && bottleneckPct && (() => {
                                    const pct = Math.min(parseFloat(bottleneckPct) / 100, 1);
                                    return (<g><rect x={x + 12} y={y + NODE_H - 6} width={NODE_W - 24} height={4} rx="2" fill={theme.barColor} opacity="0.12" /><rect x={x + 12} y={y + NODE_H - 6} width={(NODE_W - 24) * pct} height={4} rx="2" fill={theme.barColor} opacity="0.6" /></g>);
                                })()}
                                {!isHot && i === steps.length - 1 && excRateNum != null && (
                                    <g><rect x={x + 12} y={y + NODE_H - 6} width={NODE_W - 24} height={4} rx="2" fill={theme.barColor} opacity="0.12" /><rect x={x + 12} y={y + NODE_H - 6} width={(NODE_W - 24) * Math.min(excRateNum, 1)} height={4} rx="2" fill={theme.barColor} opacity="0.6" /></g>
                                )}
                                <rect x={x} y={y + 12} width={5} height={NODE_H - 24} rx="2.5" fill={theme.barColor} opacity="0.9" />
                                <circle cx={x + 28} cy={nodeCY} r={14} fill={theme.numBg} />
                                <text x={x + 28} y={nodeCY + 5} textAnchor="middle" fontSize="11" fontWeight="800" fill="#fff" fontFamily={G}>{i + 1}</text>
                                <rect x={x + NODE_W - 80} y={y - 2} width={76} height={18} rx="9" fill={theme.tagBg} stroke={theme.border} strokeWidth="0.8" />
                                <text x={x + NODE_W - 42} y={y + 11} textAnchor="middle" fontSize="7.5" fontWeight="800" fill={theme.tagText} fontFamily={G} letterSpacing="0.4">{isHot ? "🔥 " : ""}{theme.tag}</text>
                                {lines.map((line, li) => (
                                    <text key={li} x={x + 52} y={nodeCY - ((lines.length - 1) * lineH) / 2 + li * lineH - (nodeStats.length > 0 ? 12 : 4)} fontSize="12.5" fontWeight="700" fill={theme.text} fontFamily={G}>{line}</text>
                                ))}
                                {nodeStats.map((stat, si) => {
                                    const pillX = x + 52 + si * 108;
                                    const pillY = y + NODE_H - 30;
                                    return (
                                        <g key={si}>
                                            <rect x={pillX} y={pillY} width={100} height={19} rx="9.5" fill={stat.highlight ? theme.badgeBg : "rgba(0,0,0,0.07)"} opacity={stat.highlight ? 0.88 : 1} />
                                            <text x={pillX + 10} y={pillY + 13} fontSize="8.5" fontWeight="700" fill={stat.highlight ? "#fff" : theme.subText} fontFamily={G}>{stat.label}: {stat.value}</text>
                                        </g>
                                    );
                                })}
                            </g>
                        );
                    })}
                </svg>
            </Box>
            <Box sx={{ display: "flex", gap: 0.8, mt: 1, flexWrap: "wrap" }}>
                {avgE2E && <Box sx={{ background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>⏱ End-to-end: {avgE2E}</Typography></Box>}
                {excRateRaw && <Box sx={{ background: "#FAEAEA", border: "1px solid #E0A0A0", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#B03030", fontFamily: G }}>⚠ Exception rate: {excRateRaw}</Typography></Box>}
                {bottleneckActivity && bottleneckDays != null && <Box sx={{ background: "#FEE8E0", border: "1px solid #F5A58A", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#B03A1A", fontFamily: G }}>🔥 Bottleneck: {bottleneckActivity} ({bottleneckDays}d)</Typography></Box>}
                <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "6px", px: 0.9, py: 0.3 }}><Typography sx={{ fontSize: "0.6rem", color: "#9C9690", fontFamily: G }}>{steps.length} activities · {pathLabel || "process path"}</Typography></Box>
            </Box>
        </Box>
    );
}

// ── Agent Badge ───────────────────────────────────────────────────────────────

function AgentBadge({ agentUsed }) {
    if (!agentUsed) return null;
    const meta = getAgentMeta(agentUsed);
    return (
        <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, background: meta.bg, border: `1px solid ${meta.border}`, borderRadius: "6px", px: 0.9, py: 0.25 }}>
            <Typography sx={{ fontSize: "0.6rem", lineHeight: 1 }}>{meta.icon}</Typography>
            <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: meta.color, fontFamily: G, letterSpacing: "0.03em" }}>{agentUsed}</Typography>
        </Box>
    );
}

// ── Message components ────────────────────────────────────────────────────────

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

function AssistantMessage({ msg, piEvidence }) {
    // showGraph is ONLY true when the backend explicitly set graph_path.
    // The backend is the single authority — never re-derive this in the frontend.
    const showGraph = (
        msg.showGraph === true &&
        !!piEvidence?.process_path_detected &&
        !msg.isError
    );

    return (
        <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-start" }}>
            <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                <PIIcon size={14} />
            </Box>
            <Box sx={{ maxWidth: "85%", flex: 1 }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 0.6, flexWrap: "wrap" }}>
                    {msg.scopeLabel && (
                        <Box sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1, py: 0.15, display: "inline-flex", alignItems: "center", gap: 0.5 }}>
                            <svg width="8" height="8" viewBox="0 0 9 9" fill="none"><circle cx="4.5" cy="4.5" r="3.5" stroke="#B5742A" strokeWidth="1.2" /><circle cx="4.5" cy="4.5" r="1.5" fill="#B5742A" /></svg>
                            <Typography sx={{ fontSize: "0.62rem", fontWeight: 600, color: "#9C9690", fontFamily: G }}>{msg.scopeLabel}</Typography>
                        </Box>
                    )}
                    {msg.agentUsed && <AgentBadge agentUsed={msg.agentUsed} />}
                </Box>

                <Box sx={{ background: msg.isError ? "#FAEAEA" : "#FFFFFF", border: `1px solid ${msg.isError ? "#E0A0A0" : "#E8E3DA"}`, borderLeft: msg.isError ? "3px solid #B03030" : "3px solid #B5742A", borderRadius: "4px 10px 10px 10px", overflow: "hidden", boxShadow: "0 1px 4px rgba(23,20,15,0.06)" }}>
                    <Box sx={{ px: 2, py: 0.8, background: msg.isError ? "#FAEAEA" : showGraph ? "rgba(181,116,42,0.07)" : "rgba(181,116,42,0.04)", borderBottom: `1px solid ${msg.isError ? "#E0A0A0" : "#F0EDE6"}`, display: "flex", alignItems: "center", gap: 0.8 }}>
                        <Box sx={{ width: 5, height: 5, borderRadius: "50%", background: msg.isError ? "#B03030" : "#B5742A", flexShrink: 0 }} />
                        <Typography sx={{ fontSize: "0.65rem", fontWeight: 700, color: msg.isError ? "#B03030" : "#B5742A", fontFamily: G, textTransform: "uppercase", letterSpacing: "0.06em" }}>
                            {msg.isError ? "Error" : showGraph ? "Process Flow Visualization" : "Process Intelligence Finding"}
                        </Typography>
                    </Box>

                    <Box sx={{ px: 2, py: 1.4, fontSize: "0.85rem", fontFamily: G, lineHeight: 1.7, color: msg.isError ? "#B03030" : "#17140F", wordBreak: "break-word", "& p": { margin: 0, marginBottom: "8px", "&:last-child": { marginBottom: 0 } }, "& strong": { fontWeight: 700, color: "#17140F" }, "& ul, & ol": { paddingLeft: "16px", margin: "4px 0" }, "& li": { marginBottom: "3px", lineHeight: 1.6, fontSize: "0.84rem" }, "& code": { fontFamily: "monospace", fontSize: "0.78rem", background: "#F0EDE6", borderRadius: "3px", padding: "1px 4px" } }}>
                        {msg.isError ? msg.content : <ReactMarkdown>{msg.content}</ReactMarkdown>}
                    </Box>

                    {showGraph && (
                        <Box sx={{ px: 2, pb: 1.5, borderTop: "1px solid #F0EDE6", pt: 1.5 }}>
                            <ProcessGraph piEvidence={piEvidence} pathLabel={msg.pathLabel} />
                        </Box>
                    )}
                </Box>

                {/* KPI chips */}
                {piEvidence?.metrics_used && piEvidence.metrics_used.length > 0 && (
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
            <Box sx={{ width: 30, height: 30, borderRadius: "8px", background: "rgba(181,116,42,0.1)", border: "1px solid rgba(181,116,42,0.3)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                <PIIcon size={14} />
            </Box>
            <Box sx={{ background: "#FFFFFF", border: "1px solid #E8E3DA", borderLeft: "3px solid #B5742A", borderRadius: "4px 10px 10px 10px", px: 2, py: 1.2, display: "flex", gap: "5px", alignItems: "center", boxShadow: "0 1px 3px rgba(23,20,15,0.05)" }}>
                <Typography sx={{ fontSize: "0.72rem", color: "#B5742A", fontFamily: G, fontWeight: 600, mr: 1 }}>Analyzing event log</Typography>
                {[0, 0.18, 0.36].map((d, i) => (
                    <Box key={i} sx={{ width: 5, height: 5, borderRadius: "50%", background: "#B5742A", animation: "chatBounce 1.2s ease-in-out infinite", animationDelay: `${d}s` }} />
                ))}
            </Box>
            <style>{`@keyframes chatBounce{0%,80%,100%{transform:translateY(0);opacity:.4}40%{transform:translateY(-5px);opacity:1}}`}</style>
        </Box>
    );
}

// ── Right-panel components ────────────────────────────────────────────────────

function EvidenceSection({ label, children }) {
    return (
        <Box sx={{ mb: 1.5 }}>
            <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G, mb: 0.6 }}>{label}</Typography>
            {children}
        </Box>
    );
}

function ProcessEvidencePanel({ piEvidence }) {
    if (!piEvidence || !piEvidence.metrics_used) return null;
    const bn = piEvidence.bottleneck_stage;
    const dc = piEvidence.data_completeness;
    const ts = piEvidence.event_log_timespan;
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
                            <Typography sx={{ fontSize: "0.68rem", color: "#7C7670", fontFamily: G }}>
                                {bn.avg_days} days avg{bn.pct_of_total ? ` · ${bn.pct_of_total} of total cycle` : ""}{bn.case_count ? ` · ${bn.case_count} cases` : ""}
                            </Typography>
                        </Box>
                    </EvidenceSection>
                )}
                {piEvidence.process_path_detected && (
                    <EvidenceSection label={piEvidence.path_label ? "Process Path" : "Golden Path Detected"}>
                        <Box sx={{ background: "#F0FBF7", border: "1px solid #8FCFC5", borderRadius: "6px", px: 1.2, py: 0.7 }}>
                            {piEvidence.path_label && <Typography sx={{ fontSize: "0.62rem", fontWeight: 700, color: "#1A6B5E", fontFamily: G, mb: 0.3 }}>{piEvidence.path_label}</Typography>}
                            <Typography sx={{ fontSize: "0.68rem", color: "#1A6B5E", fontFamily: G, lineHeight: 1.55, wordBreak: "break-word" }}>{piEvidence.process_path_detected}</Typography>
                            {piEvidence.golden_path_percentage != null && (
                                <Typography sx={{ fontSize: "0.62rem", color: "#9C9690", fontFamily: G, mt: 0.3 }}>{piEvidence.golden_path_percentage}% of cases follow the golden path</Typography>
                            )}
                        </Box>
                    </EvidenceSection>
                )}
                {piEvidence.exception_patterns && piEvidence.exception_patterns.length > 0 && (
                    <EvidenceSection label="Exception Patterns">
                        <Stack spacing={0.4}>
                            {piEvidence.exception_patterns.map((p, i) => (
                                <Box key={i} sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#FEF3DC", border: "1px solid #F0C870", borderRadius: "6px", px: 1, py: 0.5 }}>
                                    <Typography sx={{ fontSize: "0.67rem", color: "#A05A10", fontFamily: G }}>{p.type}</Typography>
                                    <Typography sx={{ fontSize: "0.67rem", fontWeight: 700, color: "#A05A10", fontFamily: G }}>{p.case_count} cases · {p.frequency_pct}%</Typography>
                                </Box>
                            ))}
                        </Stack>
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
    if (!similarCases || similarCases.length === 0) return null;
    return (
        <Card sx={{ mb: 1.5 }}>
            <CardContent sx={{ pb: "12px !important" }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.8, mb: 1 }}>
                    <Typography sx={{ fontSize: "0.63rem", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", color: "#9C9690", fontFamily: G }}>Similar Cases</Typography>
                    <Box sx={{ background: "#17140F", borderRadius: "99px", px: 0.8, py: 0.1 }}><Typography sx={{ fontSize: "0.58rem", fontWeight: 700, color: "#F7F5F0", fontFamily: G }}>{similarCases.length}</Typography></Box>
                </Box>
                <Stack spacing={0.5}>
                    {similarCases.map((sc) => (
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
    if (!vendorData || vendorData.total_cases == null) return null;
    const excRate = parseFloat(vendorData.exception_rate_pct ?? 0);
    const overallExc = parseFloat(vendorData.overall_exception_rate_pct ?? 0);
    const avgDur = parseFloat(vendorData.avg_duration_days ?? 0);
    const overallDur = parseFloat(vendorData.overall_avg_duration_days ?? 0);
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
                    {[
                        ["Total invoices", vendorData.total_cases],
                        ["Exception rate", `${excRate}%`, excRate > overallExc + 5],
                        ["Avg cycle time", `${avgDur} days`, avgDur > overallDur + 2],
                        ["Payment terms", vendorData.payment_terms || "—"],
                    ].map(([label, value, highlight]) => (
                        <Box key={label} sx={{ display: "flex", justifyContent: "space-between", py: 0.35, borderBottom: "1px solid rgba(0,0,0,0.05)", "&:last-child": { borderBottom: "none" } }}>
                            <Typography sx={{ fontSize: "0.67rem", color: "#6B6560", fontFamily: G }}>{label}</Typography>
                            <Typography sx={{ fontSize: "0.7rem", fontWeight: 700, color: highlight ? riskColor : "#17140F", fontFamily: G }}>{value ?? "—"}</Typography>
                        </Box>
                    ))}
                </Box>
                <Box sx={{ display: "flex", gap: 0.6 }}>
                    <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 0.8, py: 0.5, textAlign: "center" }}>
                        <Typography sx={{ fontSize: "0.55rem", color: "#9C9690", fontFamily: G }}>vs avg exception</Typography>
                        <Typography sx={{ fontSize: "0.78rem", fontWeight: 800, color: riskColor, fontFamily: G }}>{(excRate - overallExc).toFixed(1) > 0 ? `+${(excRate - overallExc).toFixed(1)}` : (excRate - overallExc).toFixed(1)}%</Typography>
                    </Box>
                    <Box sx={{ flex: 1, background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "6px", px: 0.8, py: 0.5, textAlign: "center" }}>
                        <Typography sx={{ fontSize: "0.55rem", color: "#9C9690", fontFamily: G }}>vs avg cycle</Typography>
                        <Typography sx={{ fontSize: "0.78rem", fontWeight: 800, color: riskColor, fontFamily: G }}>{(avgDur - overallDur).toFixed(1) > 0 ? `+${(avgDur - overallDur).toFixed(1)}` : (avgDur - overallDur).toFixed(1)}d</Typography>
                    </Box>
                </Box>
                {vendorData.top_exception_types?.length > 0 && (
                    <Box sx={{ mt: 0.8 }}>
                        <Typography sx={{ fontSize: "0.6rem", fontWeight: 700, color: "#9C9690", fontFamily: G, mb: 0.4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Exception types</Typography>
                        <Stack spacing={0.3}>
                            {vendorData.top_exception_types.map((e, i) => (
                                <Box key={i} sx={{ display: "flex", justifyContent: "space-between", background: "rgba(255,255,255,0.7)", border: `1px solid ${riskBorder}`, borderRadius: "5px", px: 0.8, py: 0.3 }}>
                                    <Typography sx={{ fontSize: "0.64rem", color: "#3C3830", fontFamily: G }}>{e.exception_type}</Typography>
                                    <Typography sx={{ fontSize: "0.64rem", fontWeight: 700, color: riskColor, fontFamily: G }}>{e.case_count}</Typography>
                                </Box>
                            ))}
                        </Stack>
                    </Box>
                )}
            </CardContent>
        </Card>
    );
}

function DataSourcesPanel({ dataSources }) {
    const [open, setOpen] = useState(false);
    if (!dataSources || dataSources.length === 0) return null;
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
    const [piEvidence, setPiEvidence] = useState(null);
    const [similarCases, setSimilarCases] = useState(null);
    const [vendorContext, setVendorContext] = useState(null);
    const [dataSources, setDataSources] = useState([]);
    const [suggestedQs, setSuggestedQs] = useState([]);
    const [error, setError] = useState(null);
    // latestReply: text of the most recent assistant message, for PI context parsing
    const [latestReply, setLatestReply] = useState("");
    const messagesEndRef = useRef(null);

    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages, isTyping]);

    const hasReplied = messages.some(m => m.role === "assistant" && !m.isError);
    const showRight = piEvidence || similarCases || vendorContext || dataSources.length > 0 || hasReplied;

    async function handleSend(text) {
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

            const replyText = result.reply || "No response received.";

            setMessages(prev => [...prev, {
                id: Date.now() + 1,
                role: "assistant",
                content: replyText,
                ts: new Date(),
                isError: !result.success,
                scopeLabel: result.scope_label || "",
                agentUsed: result.agent_used || "",
                showGraph: !!result.graph_path,
                pathLabel: result.path_label || "",
            }]);

            // Store reply text so PIContextPanel can parse it
            if (result.success) setLatestReply(replyText);

            if (result.data_sources) setDataSources(result.data_sources);
            if (result.suggested_questions) setSuggestedQs(result.suggested_questions);
            if (result.pi_evidence) setPiEvidence(result.pi_evidence);
            if (result.similar_cases) setSimilarCases(result.similar_cases);
            if (result.vendor_context) setVendorContext(result.vendor_context);

        } catch (err) {
            const errorDetail =
                err?.response?.data?.detail
                || err?.response?.data?.error
                || err?.message
                || "Unknown PI chat error";
            setMessages(prev => [...prev, {
                id: Date.now() + 1, role: "assistant",
                content: `Unable to reach the PI backend: ${errorDetail}`,
                ts: new Date(), isError: true,
            }]);
            setError(errorDetail);
        } finally {
            setIsTyping(false);
        }
    }

    function handleKeyDown(e) {
        if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
    }

    const STARTER_QUESTIONS = [
        "Where is the biggest bottleneck in the process?",
        "Which vendors have the highest exception rates?",
        "Show me all active exceptions right now",
        "Show me the process flow for delayed invoices",
        "What is the overall exception rate and how does it compare to target?",
        "Draw the exception path in the process",
    ];

    return (
        <div className="page-container">
            <Box sx={{ pt: 4, pb: 2.5, borderBottom: "1px solid #E8E3DA", mb: 2.5 }}>
                <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
                    <Box>
                        <Typography sx={{ fontFamily: S, fontSize: "2.1rem", fontWeight: 400, color: "#17140F", letterSpacing: "-0.025em", lineHeight: 1.15, mb: 0.4 }}>
                            Process Intelligence Console
                        </Typography>
                        <Typography sx={{ fontSize: "0.85rem", color: "#9C9690", fontFamily: G }}>
                            Investigate bottlenecks, exceptions, vendor patterns, and process paths from live Celonis data
                        </Typography>
                    </Box>
                    {messages.length > 0 && (
                        <Button variant="outlined" size="small" onClick={() => {
                            setMessages([]); setPiEvidence(null); setSimilarCases(null);
                            setVendorContext(null); setDataSources([]); setSuggestedQs([]);
                            setLatestReply("");
                        }} sx={{ fontFamily: G, fontSize: "0.78rem", textTransform: "none", borderRadius: "8px" }}>
                            New Investigation
                        </Button>
                    )}
                </Box>
            </Box>

            <Grid container spacing={2.5} sx={{ height: "calc(100vh - 210px)", minHeight: 500 }}>
                {/* Chat */}
                <Grid item xs={12} md={7} sx={{ display: "flex", flexDirection: "column" }}>
                    <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                        <Box sx={{ flex: 1, overflow: "auto", px: 2.5, py: 2.5, display: "flex", flexDirection: "column", gap: 2 }}>
                            {messages.length === 0 ? (
                                <Box sx={{ margin: "auto", textAlign: "center", maxWidth: 440, py: 4 }}>
                                    <Box sx={{ width: 52, height: 52, borderRadius: "14px", background: "rgba(181,116,42,0.08)", border: "1px solid rgba(181,116,42,0.25)", display: "flex", alignItems: "center", justifyContent: "center", margin: "0 auto 16px" }}>
                                        <PIIcon size={24} />
                                    </Box>
                                    <Typography sx={{ fontFamily: S, fontSize: "1.35rem", color: "#17140F", mb: 0.6 }}>Process Intelligence Console</Typography>
                                    <Typography sx={{ fontSize: "0.8rem", color: "#9C9690", fontFamily: G, lineHeight: 1.65, mb: 2.5 }}>
                                        Every answer is grounded in your Celonis event log — cycle times, process variants, exception patterns, conformance violations, and vendor behavior.
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
                                            <Box key={t} sx={{ background: "#F0EDE6", border: "1px solid #D8D2C8", borderRadius: "99px", px: 1, py: 0.3 }}>
                                                <Typography sx={{ fontSize: "0.65rem", color: "#5C5650", fontFamily: G }}>{t}</Typography>
                                            </Box>
                                        ))}
                                    </Box>
                                </Box>
                            ) : (
                                messages.map(msg =>
                                    msg.role === "user"
                                        ? <UserMessage key={msg.id} msg={msg} />
                                        : <AssistantMessage key={msg.id} msg={msg} piEvidence={piEvidence} />
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
                            {error && <Alert severity="error" sx={{ mb: 1.5, fontSize: "0.78rem" }} onClose={() => setError(null)}>{error}</Alert>}
                            <Box sx={{ display: "flex", gap: 1.5, alignItems: "flex-end", background: "#F7F5F0", border: "1px solid #D8D2C8", borderRadius: "12px", px: 2, py: 1.2, transition: "border-color 0.15s", "&:focus-within": { borderColor: "#B5742A" } }}>
                                <Box component="textarea" value={input} onChange={e => setInput(e.target.value)} onKeyDown={handleKeyDown} disabled={isTyping}
                                    placeholder="Ask about bottlenecks, exceptions, vendors… or say 'show me the process flow'"
                                    rows={1} sx={{ flex: 1, background: "transparent", border: "none", outline: "none", resize: "none", fontFamily: G, fontSize: "0.875rem", color: "#17140F", lineHeight: 1.55, minHeight: "22px", maxHeight: "120px", overflow: "auto", "::placeholder": { color: "#9C9690" }, "&:disabled": { opacity: 0.5 } }}
                                />
                                <Button variant="contained" size="small" onClick={() => handleSend()} disabled={!input.trim() || isTyping} sx={{ flexShrink: 0, minWidth: 0, px: 1.5, py: 0.8, borderRadius: "8px" }}>
                                    {isTyping ? <CircularProgress size={14} sx={{ color: "#FFFFFF" }} /> : <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8h12M10 4l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>}
                                </Button>
                            </Box>
                            <Typography sx={{ fontSize: "0.65rem", color: "#9C9690", fontFamily: G, mt: 0.6, textAlign: "center" }}>
                                Connected to Celonis event log · Say "show me the flow" to see a process graph · Enter to send
                            </Typography>
                        </Box>
                    </Card>
                </Grid>

                {/* Right panel */}
                <Grid item xs={12} md={5} sx={{ display: "flex", flexDirection: "column" }}>
                    <Box sx={{ height: "100%", overflow: "auto", display: "flex", flexDirection: "column" }}>
                        {!showRight ? (
                            <Card sx={{ flex: 1 }}>
                                <CardContent sx={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", opacity: 0.5 }}>
                                    <Box sx={{ width: 40, height: 40, borderRadius: "10px", background: "#F0EDE6", border: "1px solid #E8E3DA", display: "flex", alignItems: "center", justifyContent: "center", mb: 1.5 }}>
                                        <PIIcon size={18} />
                                    </Box>
                                    <Typography sx={{ fontSize: "0.75rem", color: "#9C9690", fontFamily: G, textAlign: "center", maxWidth: 200, lineHeight: 1.6 }}>
                                        Process evidence, similar cases, vendor intelligence, and data sources appear here after your first question
                                    </Typography>
                                </CardContent>
                            </Card>
                        ) : (
                            <>
                                {/* PI Context Used — parsed from latest reply */}
                                <PIContextPanel replyText={latestReply} piEvidence={piEvidence} />
                                <ProcessEvidencePanel piEvidence={piEvidence} />
                                <SimilarCasesPanel similarCases={similarCases} />
                                <VendorPanel vendorData={vendorContext} />
                                <DataSourcesPanel dataSources={dataSources} />
                            </>
                        )}
                    </Box>
                </Grid>
            </Grid>
        </div>
    );
}
