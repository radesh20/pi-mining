import axios from "axios";

const api = axios.create({
  baseURL: "/api",
  timeout: 300000,
});

// -----------------------------
// Celonis Connection APIs
// -----------------------------

export const checkConnection = async () => {
  const res = await api.get("/process/connection");
  return res.data;
};

export const fetchPools = async () => {
  const res = await api.get("/process/pools");
  return res.data;
};

export const fetchTables = async () => {
  const res = await api.get("/process/tables");
  return res.data;
};

export const fetchColumns = async (tableName) => {
  const res = await api.get(`/process/columns/${encodeURIComponent(tableName)}`);
  return res.data;
};

export const fetchTableExtract = async (tableName, params = {}) => {
  const res = await api.get(`/process/table/${encodeURIComponent(tableName)}/extract`, {
    params,
  });
  return res.data;
};

export const fetchAllTablesExtract = async (params = {}) => {
  const res = await api.get("/process/extract-all-tables", { params });
  return res.data;
};

export const fetchAllTablesGroupedExtract = async (params = {}) => {
  const res = await api.get("/process/extract-all-tables-grouped", { params });
  return res.data;
};

// -----------------------------
// Process Insights APIs
// -----------------------------

export const fetchProcessInsights = async () => {
  const res = await api.get("/process/insights");
  return res.data;
};

export const fetchProcessAgents = async () => {
  const res = await api.get("/process/agents");
  return res.data;
};

export const fetchContextCoverage = async () => {
  const res = await api.get("/process/context-coverage");
  return res.data;
};

export const fetchCelonisContextLayer = async () => {
  const res = await api.get("/process/celonis-context-layer");
  return res.data;
};

export const validateWcmContext = async () => {
  const res = await api.get("/process/validate/wcm-context");
  return res.data;
};

export const refreshCache = async () => {
  const res = await api.post("/cache/refresh");
  return res.data;
};

// -----------------------------
// Prompt APIs
// -----------------------------

export const fetchAgentDeepDive = async (agentName) => {
  const res = await api.get("/prompts/deep-dive", {
    params: { agent_name: agentName },
  });
  return res.data;
};

export const fetchPromptComparison = async (agentName) => {
  const res = await api.get("/prompts/comparison", {
    params: { agent_name: agentName },
  });
  return res.data;
};

// -----------------------------
// Agent Execution APIs
// -----------------------------

export const executeInvoiceFlow = async (payload) => {
  const res = await api.post("/agents/execute-invoice", payload);
  return res.data;
};

export const executeException = async (payload) => {
  const res = await api.post("/agents/execute-exception", payload);
  return res.data;
};

export const writeAgentPrompts = async (payload) => {
  const res = await api.post("/agents/write-prompts", payload);
  return res.data;
};

export const getAutomationPolicy = async (payload) => {
  const res = await api.post("/agents/automation-policy", payload);
  return res.data;
};

export const prepareHumanReview = async (payload) => {
  const res = await api.post("/agents/human-review", payload);
  return res.data;
};

// -----------------------------
// Exception Workbench APIs
// -----------------------------

export const fetchExceptionCategories = async () => {
  const res = await api.get("/exceptions/categories");
  return res.data;
};

export const fetchExceptionRecords = async (exceptionType) => {
  const res = await api.get("/exceptions/records", {
    params: { type: exceptionType },
  });
  return res.data;
};

export const analyzeExceptionRecord = async (payload) => {
  const res = await api.post("/exceptions/analyze", payload);
  return res.data;
};

export const fetchNextBestAction = async (analysis) => {
  const res = await api.post("/exceptions/next-best-action", { analysis });
  return res.data;
};

export const sendExceptionToTeams = async (analysis) => {
  const res = await api.post("/exceptions/send-human-review", { analysis });
  return res.data;
};

export default api;
