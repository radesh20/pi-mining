import React, { useEffect, useState } from "react";
import Typography from "@mui/material/Typography";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import TableSelector from "../components/TableSelector";
import LoadingSpinner from "../components/LoadingSpinner";
import { checkConnection, fetchPools } from "../api/client";

export default function CelonisSetup() {
  const [connection, setConnection] = useState(null);
  const [pools, setPools] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const load = async () => {
      try {
        const connRes = await checkConnection();
        setConnection(connRes.data);

        const poolRes = await fetchPools();
        setPools(poolRes.data || []);
      } catch (err) {
        setError(err.response?.data?.detail || err.message);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, []);

  if (loading) {
    return <LoadingSpinner message="Connecting to Celonis..." />;
  }

  return (
    <div className="page-container">
      <Typography
        variant="h4"
        sx={{ fontWeight: 700, mb: 3, color: "#1f2937" }}
      >
        Celonis Connection
      </Typography>

      {error && (
        <div className="error-box">
          <Typography variant="h6">Connection Failed</Typography>
          <Typography variant="body2">{error}</Typography>
          <Typography variant="body2" sx={{ mt: 1 }}>
            Check CELONIS_BASE_URL, CELONIS_API_TOKEN,
            CELONIS_DATA_POOL_ID, and CELONIS_DATA_MODEL_ID in
            backend/.env
          </Typography>
        </div>
      )}

      {connection && (
        <Card
          sx={{
            background: "#ffffff",
            border: "1px solid #d9efe2",
            borderRadius: 2,
            mb: 3,
          }}
        >
          <CardContent>
            <Box
              sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}
            >
              <Typography
                variant="h6"
                sx={{ color: "#2e7d32", fontWeight: 700 }}
              >
                Connected to Celonis
              </Typography>
              <Chip label="LIVE" color="success" size="small" />
            </Box>

            <Typography variant="body2" sx={{ color: "#374151" }}>
              Base URL: {connection.base_url}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Data Pool ID: {connection.data_pool_id}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Data Model ID: {connection.data_model_id}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Data Model Name: {connection.data_model_name}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Activity Table: {connection.activity_table}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Case Column: {connection.case_column}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Activity Column: {connection.activity_column}
            </Typography>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              Timestamp Column: {connection.timestamp_column}
            </Typography>
          </CardContent>
        </Card>
      )}

      {pools.length > 0 && (
        <Box sx={{ mb: 3 }}>
          <Typography
            variant="h6"
            sx={{ fontWeight: 700, color: "#1976d2", mb: 1 }}
          >
            Data Pools
          </Typography>

          {pools.map((pool, i) => (
            <Card
              key={i}
              sx={{
                background: "#ffffff",
                border: "1px solid #e5e7eb",
                borderRadius: 2,
                mb: 1,
              }}
            >
              <CardContent>
                <Typography variant="subtitle1" sx={{ color: "#1f2937", fontWeight: 600 }}>
                  {pool.pool_name}{" "}
                  <span style={{ color: "#6b7280", fontSize: "0.8rem" }}>
                    ({pool.pool_id})
                  </span>
                </Typography>

                <Box
                  sx={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 0.5,
                    mt: 1,
                  }}
                >
                  {(pool.models || []).map((m, j) => (
                    <Chip
                      key={j}
                      label={m.model_name}
                      size="small"
                      variant="outlined"
                    />
                  ))}
                </Box>
              </CardContent>
            </Card>
          ))}
        </Box>
      )}

      {connection && <TableSelector />}
    </div>
  );
}
