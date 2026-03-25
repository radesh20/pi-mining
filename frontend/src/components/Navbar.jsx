import React from "react";
import { Link, useLocation } from "react-router-dom";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Box from "@mui/material/Box";

const navItems = [
  { label: "Dashboard", path: "/" },
  { label: "Celonis Setup", path: "/setup" },
  { label: "Process Agents", path: "/process-agents" },
  { label: "Exceptions Workbench", path: "/exceptions-workbench" },
  { label: "Agent Deep Dive", path: "/deep-dive" },
  { label: "Vendor Analysis", path: "/vendor-analysis" },
  { label: "Cross-Agent Flow", path: "/interaction" },
];

export default function Navbar() {
  const location = useLocation();
  return (
    <AppBar
      position="fixed"
      elevation={0}
      sx={{ background: "#ffffff", borderBottom: "1px solid #e5e7eb", color: "#1f2937" }}
    >
      <Toolbar>
        <Typography variant="h6" sx={{ flexGrow: 0, mr: 4, fontWeight: 700, color: "#1f2937" }}>
          PI AI Workbench
        </Typography>
        <Box sx={{ flexGrow: 1, display: "flex", gap: 1 }}>
          {navItems.map((item) => (
            <Button
              key={item.path}
              component={Link}
              to={item.path}
              variant={location.pathname === item.path ? "contained" : "text"}
              size="small"
              sx={{
                textTransform: "none",
                color: location.pathname === item.path ? "#fff" : "#374151",
              }}
            >
              {item.label}
            </Button>
          ))}
        </Box>
      </Toolbar>
    </AppBar>
  );
}
