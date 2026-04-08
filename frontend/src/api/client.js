import axios from "axios";

const api = axios.create({
  baseURL: "/api",
  timeout: 300000,
});

let cacheStatusInFlight = null;
let cacheStatusSnapshot = null;
let cacheStatusSnapshotAt = 0;
let waitForCacheReadyPromise = null;
let waitForCacheReadyController = null;
let waitForCacheReadyConsumers = 0;
const CACHE_STATUS_TTL_MS = 5000;
const CACHE_STATUS_MIN_POLL_MS = 5000;
const CACHE_STATUS_MAX_POLL_ATTEMPTS = 20;
const CACHE_LOADING_SLOW_MESSAGE = "Cache loading taking longer than expected";
const inFlightRequests = new Map();

const stableStringify = (value) => {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
};

const dedupeRequest = (key, factory) => {
  if (inFlightRequests.has(key)) {
    return inFlightRequests.get(key);
  }

  const request = Promise.resolve()
    .then(factory)
    .finally(() => {
      inFlightRequests.delete(key);
    });

  inFlightRequests.set(key, request);
  return request;
};

export const unwrapApiData = (payload) => {
  if (payload == null) return payload;
  if (payload.data?.data !== undefined) return payload.data.data;
  if (payload.success !== undefined && payload.data !== undefined) return payload.data;
  if (payload.data !== undefined && !Array.isArray(payload)) return payload.data;
  return payload;
};

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

export const refreshCache = async ({ background = false, fullReload = false } = {}) => {
  const res = await api.post("/cache/refresh", null, {
    params: { background, full_reload: fullReload },
  });
  return res.data;
};

export const fetchCacheStatus = async ({ force = false } = {}) => {
  const now = Date.now();
  if (!force && cacheStatusSnapshot && now - cacheStatusSnapshotAt < CACHE_STATUS_TTL_MS) {
    return cacheStatusSnapshot;
  }
  if (!force && cacheStatusInFlight) {
    return cacheStatusInFlight;
  }

  cacheStatusInFlight = api.get("/cache/status")
    .then((res) => {
      cacheStatusSnapshot = res.data;
      cacheStatusSnapshotAt = Date.now();
      return res.data;
    })
    .finally(() => {
      cacheStatusInFlight = null;
    });

  return cacheStatusInFlight;
};

const createAbortError = () => {
  const error = new Error("Cache status polling aborted.");
  error.name = "AbortError";
  return error;
};

const pollUntilCacheReady = ({ timeoutMs = 120000, pollMs = CACHE_STATUS_MIN_POLL_MS, signal } = {}) => {
  const effectivePollMs = Math.max(Number(pollMs || 0), CACHE_STATUS_MIN_POLL_MS);
  const startedAt = Date.now();
  let attempts = 0;
  let intervalId = null;
  let inTick = false;
  let finished = false;
  let abortHandler = null;

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      if (intervalId) clearInterval(intervalId);
      intervalId = null;
      if (signal && abortHandler) {
        signal.removeEventListener("abort", abortHandler);
      }
      abortHandler = null;
    };

    const complete = (fn, value) => {
      if (finished) return;
      finished = true;
      cleanup();
      fn(value);
    };

    const onError = (error) => complete(reject, error);
    const onResolve = (value) => complete(resolve, value);

    if (signal?.aborted) {
      onError(createAbortError());
      return;
    }

    const tick = async () => {
      if (inTick || finished) return;
      inTick = true;
      try {
        attempts += 1;
        const status = unwrapApiData(await fetchCacheStatus({ force: true })) || {};
        if (status.is_loaded && !status.refresh_in_progress) {
          onResolve(status);
          return;
        }
        if (attempts >= CACHE_STATUS_MAX_POLL_ATTEMPTS) {
          onError(new Error(CACHE_LOADING_SLOW_MESSAGE));
          return;
        }
        if (Date.now() - startedAt >= timeoutMs) {
          onError(new Error("Timed out waiting for analytics cache to finish loading."));
        }
      } catch (error) {
        onError(error);
      } finally {
        inTick = false;
      }
    };

    abortHandler = () => onError(createAbortError());
    if (signal) {
      signal.addEventListener("abort", abortHandler, { once: true });
      if (signal.aborted) {
        onError(createAbortError());
        return;
      }
    }

    intervalId = setInterval(tick, effectivePollMs);
    tick();
  });
};

export const waitForCacheReady = async ({ timeoutMs = 120000, pollMs = CACHE_STATUS_MIN_POLL_MS, signal } = {}) => {
  if (!waitForCacheReadyPromise) {
    waitForCacheReadyController = new AbortController();
    waitForCacheReadyPromise = pollUntilCacheReady({
      timeoutMs,
      pollMs,
      signal: waitForCacheReadyController.signal,
    }).finally(() => {
      waitForCacheReadyPromise = null;
      waitForCacheReadyController = null;
      waitForCacheReadyConsumers = 0;
    });
  }

  waitForCacheReadyConsumers += 1;

  try {
    if (!signal) {
      return await waitForCacheReadyPromise;
    }
    if (signal.aborted) {
      throw createAbortError();
    }
    const signalAbortPromise = new Promise((_, reject) => {
      const onAbort = () => {
        reject(createAbortError());
      };
      signal.addEventListener("abort", onAbort, { once: true });
      waitForCacheReadyPromise.finally(() => signal.removeEventListener("abort", onAbort));
    });
    return await Promise.race([waitForCacheReadyPromise, signalAbortPromise]);
  } finally {
    waitForCacheReadyConsumers = Math.max(0, waitForCacheReadyConsumers - 1);
    if (waitForCacheReadyConsumers === 0 && waitForCacheReadyController) {
      waitForCacheReadyController.abort();
    }
  }
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
  return dedupeRequest(
    `POST:/agents/execute-invoice:${stableStringify(payload || {})}`,
    async () => {
      const res = await api.post("/agents/execute-invoice", payload);
      return res.data;
    }
  );
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
  return dedupeRequest("GET:/exceptions/categories", async () => {
    const res = await api.get("/exceptions/categories");
    return res.data;
  });
};

export const fetchExceptionRecords = async (exceptionType) => {
  return dedupeRequest(`GET:/exceptions/records:${stableStringify({ type: exceptionType })}`, async () => {
    const res = await api.get("/exceptions/records", {
      params: { type: exceptionType },
    });
    return res.data;
  });
};

export const fetchAllExceptionRecords = async () => {
  return dedupeRequest(`GET:/exceptions/records:${stableStringify({ type: "*" })}`, async () => {
    const res = await api.get("/exceptions/records", {
      params: { type: "*" },
    });
    return res.data;
  });
};

export const fetchExceptionWorkbenchData = async () => {
  return dedupeRequest("GET:/exceptions/workbench-data", async () => {
    const res = await api.get("/exceptions/workbench-data");
    return res.data;
  });
};

export const analyzeExceptionRecord = async (payload) => {
  return dedupeRequest(
    `POST:/exceptions/analyze:${stableStringify(payload || {})}`,
    async () => {
      const res = await api.post("/exceptions/analyze", payload);
      return res.data;
    }
  );
};

export const fetchNextBestAction = async (analysis) => {
  const res = await api.post("/exceptions/next-best-action", { analysis });
  return res.data;
};

export const sendExceptionToTeams = async (analysis) => {
  const res = await api.post("/exceptions/send-human-review", { analysis });
  return res.data;
};

// -----------------------------
// Chat APIs
// -----------------------------

export const sendChatMessage = async ({ message, caseId, vendorId, conversationHistory = [] }) => {
  const res = await api.post("/chat/", {
    message,
    case_id: caseId || null,
    vendor_id: vendorId || null,
    conversation_history: conversationHistory.map((m) => ({
      role: m.role,
      content: m.content,
    })),
  });
  // ChatResponse now returns these fields directly at top level:
  // { success, reply, sql_comparison, pi_advantage, agent_action, process_stage, follow_ups, context_used, error }
  return res.data;
};

export const sendSqlChatMessage = async ({
  message,
  tableName,
  dialect = "PostgreSQL",
  caseId,
  vendorId,
  conversationHistory = [],
}) => {
  const res = await api.post("/chat/sql", {
    message,
    table_name: tableName || null,
    dialect,
    case_id: caseId || null,
    vendor_id: vendorId || null,
    conversation_history: conversationHistory.map((m) => ({
      role: m.role,
      content: m.content,
    })),
  });
  return res.data;
};

export default api;
