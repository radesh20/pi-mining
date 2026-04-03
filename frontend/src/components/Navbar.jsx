import React, { useState, useEffect } from "react";
import { Link, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { label: "Celonis Setup", path: "/setup" },
  { label: "Exception Triage", path: "/exceptions-workbench" },
  { label: "Cross-Agent Flow", path: "/interaction" },
  { label: "PI Chat", path: "/chat", highlight: true },
  { label: "SQL Chatbot", path: "/sql-chat", highlight: true },
];

export default function Navbar() {
  const location = useLocation();
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <nav
      style={{
        position: "fixed",
        top: 0, left: 0, right: 0,
        zIndex: 1000,
        height: "56px",
        background: scrolled ? "rgba(247,245,240,0.95)" : "#F7F5F0",
        backdropFilter: scrolled ? "blur(12px)" : "none",
        WebkitBackdropFilter: scrolled ? "blur(12px)" : "none",
        borderBottom: `1px solid ${scrolled ? "#D8D2C8" : "#E8E3DA"}`,
        transition: "background 0.2s, border-color 0.2s",
      }}
    >
      <div style={{
        maxWidth: "1380px",
        margin: "0 auto",
        padding: "0 24px",
        height: "100%",
        display: "flex",
        alignItems: "center",
        gap: "0",
      }}>
        {/* Logo */}
        <Link to="/" style={{ textDecoration: "none", marginRight: "28px", flexShrink: 0, display: "flex", alignItems: "center", gap: "9px" }}>
          <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
            <path d="M11 2L20 7V15L11 20L2 15V7L11 2Z" fill="#B5742A" fillOpacity="0.15" stroke="#B5742A" strokeWidth="1.5" />
            <path d="M11 6L16 9V13L11 16L6 13V9L11 6Z" fill="#B5742A" />
          </svg>
          <span style={{
            fontFamily: "'Instrument Serif', Georgia, serif",
            fontWeight: 400,
            fontSize: "1.05rem",
            color: "#17140F",
            letterSpacing: "-0.01em",
            lineHeight: 1,
          }}>
            PI AI Workbench
          </span>
        </Link>

        {/* Vertical separator */}
        <div style={{ width: "1px", height: "20px", background: "#E8E3DA", marginRight: "24px", flexShrink: 0 }} />

        {/* Nav links */}
        <div style={{ display: "flex", alignItems: "center", gap: "2px", overflow: "auto", flex: 1 }}>
          {NAV_ITEMS.map((item) => {
            const active = location.pathname === item.path;
            const isHighlight = item.highlight;

            if (isHighlight) {
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  style={{
                    position: "relative",
                    display: "inline-flex",
                    alignItems: "center",
                    gap: "6px",
                    padding: "5px 12px",
                    borderRadius: "8px",
                    fontSize: "0.8rem",
                    fontWeight: 600,
                    color: active ? "#FFFFFF" : "#B5742A",
                    textDecoration: "none",
                    background: active ? "#B5742A" : "rgba(181,116,42,0.08)",
                    border: `1px solid ${active ? "#B5742A" : "rgba(181,116,42,0.25)"}`,
                    transition: "all 0.15s",
                    whiteSpace: "nowrap",
                    fontFamily: "'Geist', system-ui, sans-serif",
                    letterSpacing: "-0.01em",
                    marginLeft: "6px",
                  }}
                  onMouseEnter={e => {
                    if (!active) {
                      e.currentTarget.style.background = "rgba(181,116,42,0.15)";
                      e.currentTarget.style.borderColor = "rgba(181,116,42,0.4)";
                    }
                  }}
                  onMouseLeave={e => {
                    if (!active) {
                      e.currentTarget.style.background = "rgba(181,116,42,0.08)";
                      e.currentTarget.style.borderColor = "rgba(181,116,42,0.25)";
                    }
                  }}
                >
                  {/* Chat icon */}
                  <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
                    <path d="M11 1H2C1.45 1 1 1.45 1 2v7c0 .55.45 1 1 1h1.5l1.5 2 1.5-2H11c.55 0 1-.45 1-1V2c0-.55-.45-1-1-1Z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" />
                    <path d="M4 5h5M4 7.5h3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
                  </svg>
                  {item.label}
                </Link>
              );
            }

            return (
              <Link
                key={item.path}
                to={item.path}
                style={{
                  position: "relative",
                  display: "inline-flex",
                  alignItems: "center",
                  padding: "6px 11px",
                  borderRadius: "8px",
                  fontSize: "0.8rem",
                  fontWeight: active ? 600 : 400,
                  color: active ? "#17140F" : "#7A746E",
                  textDecoration: "none",
                  background: active ? "#EDE9E1" : "transparent",
                  transition: "background 0.15s, color 0.15s",
                  whiteSpace: "nowrap",
                  fontFamily: "'Geist', system-ui, sans-serif",
                  letterSpacing: "-0.01em",
                }}
                onMouseEnter={e => { if (!active) { e.currentTarget.style.background = "#F0EDE6"; e.currentTarget.style.color = "#17140F"; } }}
                onMouseLeave={e => { if (!active) { e.currentTarget.style.background = "transparent"; e.currentTarget.style.color = "#7A746E"; } }}
              >
                {item.label}
                {active && (
                  <span style={{
                    position: "absolute",
                    bottom: "4px",
                    left: "50%",
                    transform: "translateX(-50%)",
                    width: "14px",
                    height: "2px",
                    borderRadius: "99px",
                    background: "#B5742A",
                  }} />
                )}
              </Link>
            );
          })}
        </div>

        {/* Live indicator */}
        <div style={{ display: "flex", alignItems: "center", gap: "6px", flexShrink: 0, marginLeft: "16px" }}>
          <span style={{
            width: "6px", height: "6px",
            borderRadius: "50%",
            background: "#1A6B5E",
            boxShadow: "0 0 0 2px rgba(26,107,94,0.2)",
            animation: "pulse 2s ease infinite",
          }} />
          <span style={{ fontSize: "0.72rem", color: "#9C9690", fontWeight: 500, letterSpacing: "0.02em" }}>
            Live
          </span>
        </div>
      </div>

      <style>{`
        @keyframes pulse {
          0%, 100% { box-shadow: 0 0 0 2px rgba(26,107,94,0.2); }
          50% { box-shadow: 0 0 0 4px rgba(26,107,94,0.1); }
        }
      `}</style>
    </nav>
  );
}
