# Process Mining Agents

PI-first demo app for Celonis Working Capital Management + Azure OpenAI.

## Canonical config

- Use `backend/.env` as the single source of truth for backend configuration.
- `backend/app/.env` is intentionally a placeholder and should not contain secrets.
- If you rotate keys, update `backend/.env` only.

## Quick start

- Start both apps: `make run-dev`
- Start backend only: `make run-backend`
- Start frontend only: `make run-frontend`

## WCM refresh loop

- Refresh cache: `make refresh-cache`
- Validate WCM context: `make validate-wcm`
- Live smoke pass: `make smoke-live`
- Coverage endpoint: `http://localhost:8000/api/process/context-coverage`
- Grouped WCM extract endpoint: `http://localhost:8000/api/process/working-capital/extract-grouped`
- Runtime tuning endpoint: `http://localhost:8000/api/process/runtime-tuning`

## Grouped extract safety knobs

- `WCM_ENABLE_GROUPED_EXTRACT=true` enables grouped `t_o_custom_*` + `t_e_custom_*` extraction.
- `WCM_GROUPED_MAX_TABLES` and `WCM_GROUPED_MAX_ROWS_PER_TABLE` cap extraction load for runtime stability.
- `WCM_GROUPED_SAMPLE_MAX_ROWS` caps rows pulled per table in sample mode (`include_rows=false`).
- `WCM_GROUPED_TABLE_ALLOWLIST` lets you pin high-value tables while still including required core tables.

## Runtime stability knobs

- `CACHE_STALE_WHILE_REFRESH=true` serves cached data immediately while background refresh runs.
- `CACHE_REFRESH_WAIT_SECONDS` controls max wait when a refresh is already in progress.
- `CACHE_INITIAL_LOAD_WAIT_SECONDS` controls max wait for first-load cache warmup.
- `CACHE_REFRESH_HARD_TIMEOUT_SECONDS` resets a stuck refresh lock after max duration.
- `CACHE_AUTO_REFRESH_POLICY=stale_only` avoids unnecessary periodic refresh calls.
- `CELONIS_DISCOVERY_CACHE_TTL_SECONDS` caches table/column metadata lookups.
- `CELONIS_DISCOVERY_MAX_TABLES` caps full discovery scans in high-latency tenants.

## Demo flow

- Scripted order and talking points: `docs/demo-walkthrough.md`

## Verification

- Verify backend syntax: `make verify-backend`
- Build frontend: `make build-frontend`

## Notes

- Backend API runs on `http://localhost:8000`
- Frontend runs on `http://localhost:3000`
- The current WCM OLAP source is `t_o_custom_AccountingDocumentSegment`
