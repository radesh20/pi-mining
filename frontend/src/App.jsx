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
    primary: { main: "#1976d2" },
    success: { main: "#2e7d32" },
    warning: { main: "#ed6c02" },
    error: { main: "#d32f2f" },
    background: { default: "#f7f9fc", paper: "#ffffff" },
    text: { primary: "#1f2937", secondary: "#6b7280" },
  },
  typography: { fontFamily: "'Segoe UI', 'Inter', sans-serif" },
  shape: { borderRadius: 10 },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: "#ffffff",
          border: "1px solid #e5e7eb",
          boxShadow: "0 1px 2px rgba(16,24,40,0.06)",
          transition: "box-shadow 0.2s ease, transform 0.2s ease",
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: "none",
          fontWeight: 600,
        },
      },
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <Navbar />
        <div style={{ padding: "24px", marginTop: "64px" }}>
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
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;
