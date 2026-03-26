import React from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";
import CelonisSetup from "./pages/CelonisSetup";
import ProcessAgentsView from "./pages/ProcessAgentsView";
import ExceptionsWorkbench from "./pages/ExceptionsWorkbench";
import AgentDeepDive from "./pages/AgentDeepDive";
import CrossAgentInteraction from "./pages/CrossAgentInteraction";
import VendorAnalysis from "./pages/VendorAnalysis";
import "./App.css";

const theme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#B5742A" },
    success: { main: "#1A6B5E" },
    warning: { main: "#A05A10" },
    error: { main: "#B03030" },
    info: { main: "#1E4E8C" },
    background: { default: "#F7F5F0", paper: "#FFFFFF" },
    text: { primary: "#17140F", secondary: "#5C5650" },
    divider: "#E8E3DA",
  },
  typography: {
    fontFamily: "'Geist', system-ui, sans-serif",
    h1: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400 },
    h2: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400 },
    h3: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400 },
    h4: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400, letterSpacing: "-0.02em" },
    h5: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400 },
    h6: { fontFamily: "'Instrument Serif', Georgia, serif", fontWeight: 400, letterSpacing: "-0.01em" },
    button: { fontWeight: 600, textTransform: "none", letterSpacing: 0 },
  },
  shape: { borderRadius: 10 },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: { background: "#F7F5F0" },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: "#FFFFFF",
          border: "1px solid #E8E3DA",
          boxShadow: "0 1px 4px rgba(23,20,15,0.07)",
          borderRadius: "14px",
          transition: "box-shadow 0.2s, transform 0.2s",
          "&:hover": { boxShadow: "0 4px 12px rgba(23,20,15,0.08)" },
        },
      },
    },
    MuiCardContent: {
      styleOverrides: {
        root: { padding: "20px", "&:last-child": { paddingBottom: "20px" } },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: "none",
          fontWeight: 600,
          borderRadius: "10px",
          fontSize: "0.82rem",
          letterSpacing: 0,
          boxShadow: "none",
          "&:hover": { boxShadow: "none" },
        },
        containedPrimary: {
          background: "#B5742A",
          "&:hover": { background: "#9A6020", boxShadow: "none" },
        },
        outlinedPrimary: {
          borderColor: "#D8D2C8",
          color: "#5C5650",
          "&:hover": { background: "#F5F2EC", borderColor: "#C4BDB0" },
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          fontFamily: "'Geist', system-ui, sans-serif",
          fontWeight: 600,
          fontSize: "0.7rem",
          borderRadius: "99px",
          letterSpacing: "0.02em",
        },
        colorError: { background: "#FAEAEA", color: "#B03030", border: "1px solid #E0A0A0" },
        colorWarning: { background: "#FEF3DC", color: "#A05A10", border: "1px solid #F0C870" },
        colorSuccess: { background: "#E0F0E8", color: "#1D5C3A", border: "1px solid #80C0A0" },
        colorInfo: { background: "#EBF2FC", color: "#1E4E8C", border: "1px solid #90B8E8" },
      },
    },
    MuiAlert: {
      styleOverrides: {
        root: {
          borderRadius: "10px",
          fontFamily: "'Geist', system-ui, sans-serif",
          fontSize: "0.875rem",
        },
        standardInfo: { background: "#EBF2FC", color: "#1E4E8C" },
        standardSuccess: { background: "#E0F0E8", color: "#1D5C3A" },
        standardWarning: { background: "#FEF3DC", color: "#A05A10" },
        standardError: { background: "#FAEAEA", color: "#B03030" },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: {
          "& .MuiTableCell-head": {
            background: "#F0EDE6",
            color: "#9C9690",
            fontWeight: 700,
            fontSize: "0.69rem",
            textTransform: "uppercase",
            letterSpacing: "0.07em",
            borderBottom: "1px solid #E8E3DA",
            fontFamily: "'Geist', system-ui, sans-serif",
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        body: {
          borderBottom: "1px solid #E8E3DA",
          color: "#5C5650",
          fontFamily: "'Geist', system-ui, sans-serif",
          fontSize: "0.875rem",
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          "&:hover .MuiTableCell-body": { background: "#F5F2EC" },
          "&:last-child .MuiTableCell-body": { borderBottom: "none" },
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          "& .MuiOutlinedInput-root": {
            borderRadius: "10px",
            background: "#FFFFFF",
            fontFamily: "'Geist', system-ui, sans-serif",
            fontSize: "0.875rem",
            "& fieldset": { borderColor: "#D8D2C8" },
            "&:hover fieldset": { borderColor: "#C4BDB0" },
            "&.Mui-focused fieldset": { borderColor: "#B5742A" },
          },
          "& .MuiInputLabel-root": {
            fontFamily: "'Geist', system-ui, sans-serif",
            fontSize: "0.82rem",
            color: "#9C9690",
          },
        },
      },
    },
    MuiDivider: {
      styleOverrides: { root: { borderColor: "#E8E3DA" } },
    },
    MuiAppBar: {
      styleOverrides: { root: { display: "none" } },
    },
  },
});

export default function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <Navbar />
        <main style={{ paddingTop: "56px" }}>
          <div style={{ padding: "0 24px" }}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/setup" element={<CelonisSetup />} />
              <Route path="/process-agents" element={<ProcessAgentsView />} />
              <Route path="/exceptions-workbench" element={<ExceptionsWorkbench />} />
              <Route path="/deep-dive" element={<AgentDeepDive />} />
              <Route path="/vendor-analysis" element={<VendorAnalysis />} />
              <Route path="/interaction" element={<CrossAgentInteraction />} />
            </Routes>
          </div>
        </main>
      </BrowserRouter>
    </ThemeProvider>
  );
}