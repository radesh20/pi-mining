import React, { useEffect, useState } from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import { fetchTables, fetchColumns, fetchTableExtract, fetchAllTablesExtract } from "../api/client";

export default function TableSelector() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [columns, setColumns] = useState([]);
  const [tableExtract, setTableExtract] = useState(null);
  const [allTablesExtract, setAllTablesExtract] = useState(null);
  const [extractLoading, setExtractLoading] = useState(false);

  useEffect(() => {
    fetchTables()
      .then((res) => setTables(res.data || []))
      .catch((err) => console.error(err));
  }, []);

  const handleTableClick = async (tableName) => {
    setSelectedTable(tableName);
    setAllTablesExtract(null);
    setTableExtract(null);
    setExtractLoading(true);
    try {
      const [colsRes, extractRes] = await Promise.all([
        fetchColumns(tableName),
        fetchTableExtract(tableName, { include_rows: false }),
      ]);
      setColumns(colsRes.data || []);
      setTableExtract(extractRes.data || null);
    } catch (err) {
      console.error(err);
      setColumns([]);
      setTableExtract(null);
    } finally {
      setExtractLoading(false);
    }
  };

  const handleFetchAll = async () => {
    setSelectedTable("ALL_TABLES");
    setColumns([]);
    setTableExtract(null);
    setAllTablesExtract(null);
    setExtractLoading(true);
    try {
      const res = await fetchAllTablesExtract({ include_rows: false });
      setAllTablesExtract(res.data || {});
    } catch (err) {
      console.error(err);
    } finally {
      setExtractLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h6" sx={{ fontWeight: 700, color: "#1f2937", mb: 2 }}>
        Data Model Tables
      </Typography>
      <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1, mb: 2 }}>
        <Chip
          label="Fetch All Tables"
          onClick={handleFetchAll}
          variant={selectedTable === "ALL_TABLES" ? "filled" : "outlined"}
          color={selectedTable === "ALL_TABLES" ? "secondary" : "default"}
          sx={{
            cursor: "pointer",
            fontWeight: "bold",
            borderColor: selectedTable === "ALL_TABLES" ? "#9c27b0" : "#d1d5db",
            background: selectedTable === "ALL_TABLES" ? "#9c27b0" : "#fdf4ff",
            color: selectedTable === "ALL_TABLES" ? "#fff" : "#86198f",
          }}
        />
        {tables.map((t, i) => (
          <Chip
            key={i}
            label={t.table_name}
            onClick={() => handleTableClick(t.table_name)}
            variant={selectedTable === t.table_name ? "filled" : "outlined"}
            color={selectedTable === t.table_name ? "primary" : "default"}
            sx={{
              cursor: "pointer",
              borderColor: selectedTable === t.table_name ? "#1976d2" : "#d1d5db",
              background: selectedTable === t.table_name ? "#1976d2" : "#fff",
              color: selectedTable === t.table_name ? "#fff" : "#374151",
            }}
          />
        ))}
      </Box>
      {extractLoading && (
        <Card
          sx={{
            borderRadius: 2,
            border: "1px solid #e5e7eb",
          }}
        >
          <CardContent sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <CircularProgress size={18} />
            <Typography variant="body2" sx={{ color: "#6b7280" }}>
              Extracting table rows and amount summary...
            </Typography>
          </CardContent>
        </Card>
      )}

      {selectedTable && columns.length > 0 && !extractLoading && (
        <Card
          sx={{
            borderRadius: 2,
            border: "1px solid #e5e7eb",
            "&:hover": { boxShadow: "0 10px 24px rgba(15, 23, 42, 0.08)" },
          }}
        >
          <CardContent>
            <Typography variant="subtitle2" sx={{ color: "#2e7d32", mb: 1, fontWeight: 700 }}>
              Columns in {selectedTable}
            </Typography>
            {tableExtract && (
              <Box sx={{ mb: 1.2 }}>
                <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                  Rows extracted: <strong>{tableExtract.row_count || 0}</strong>
                </Typography>
                <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                  Amount column detected:{" "}
                  <strong>{tableExtract.amount_summary?.amount_column_detected || "N/A"}</strong>
                </Typography>
                {Object.keys(tableExtract.amount_summary?.totals_by_column || {}).length > 0 && (
                  <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
                    {Object.entries(tableExtract.amount_summary.totals_by_column).map(([col, total]) => (
                      <Chip
                        key={col}
                        size="small"
                        label={`${col}: ${Number(total || 0).toLocaleString()}`}
                        sx={{ background: "#eff6ff", color: "#1e3a8a", border: "1px solid #bfdbfe" }}
                      />
                    ))}
                  </Box>
                )}
              </Box>
            )}
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
              {columns.map((col, i) => (
                <span key={i} className="evidence-tag">{col}</span>
              ))}
            </Box>
          </CardContent>
        </Card>
      )}

      {selectedTable === "ALL_TABLES" && allTablesExtract && !extractLoading && (
        <Card
          sx={{
            borderRadius: 2,
            border: "1px solid #e5e7eb",
          }}
        >
          <CardContent>
            <Typography variant="subtitle2" sx={{ color: "#9c27b0", mb: 2, fontWeight: 700 }}>
              All Tables Summary
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151", mb: 2 }}>
              Total tables extracted: <strong>{Object.keys(allTablesExtract).length}</strong>
            </Typography>
            {Object.entries(allTablesExtract).map(([tbName, tbData]) => (
              <Box key={tbName} sx={{ mb: 2, p: 1.5, border: "1px solid #f3e8ff", borderRadius: 2, background: "#faf5ff" }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, color: "#7e22ce", mb: 1 }}>
                  {tbName}
                </Typography>
                <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                  Rows extracted: <strong>{tbData.row_count || 0}</strong>
                </Typography>
                <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                  Amount column detected:{" "}
                  <strong>{tbData.amount_summary?.amount_column_detected || "N/A"}</strong>
                </Typography>
                {Object.keys(tbData.amount_summary?.totals_by_column || {}).length > 0 && (
                  <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 1 }}>
                    {Object.entries(tbData.amount_summary.totals_by_column).map(([col, total]) => (
                      <Chip
                        key={col}
                        size="small"
                        label={`${col}: ${Number(total || 0).toLocaleString()}`}
                        sx={{ background: "#f3e8ff", color: "#6b21a8", border: "1px solid #d8b4fe" }}
                      />
                    ))}
                  </Box>
                )}
              </Box>
            ))}
          </CardContent>
        </Card>
      )}
    </Box>
  );
}
