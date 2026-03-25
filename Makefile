SHELL := /bin/zsh

.PHONY: help run-backend run-frontend run-dev refresh-cache validate-wcm build-frontend verify-backend smoke-live

help:
	@echo "Available targets:"
	@echo "  make run-backend    - start FastAPI backend on :8000"
	@echo "  make run-frontend   - start Vite frontend on :3000"
	@echo "  make run-dev        - run backend and frontend together"
	@echo "  make refresh-cache  - refresh Celonis/WCM cache"
	@echo "  make validate-wcm   - run WCM validation endpoint"
	@echo "  make verify-backend - compile backend Python files"
	@echo "  make build-frontend - production build for frontend"
	@echo "  make smoke-live     - refresh + smoke critical live endpoints"

run-backend:
	cd backend && ./venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

run-frontend:
	cd frontend && npm run dev -- --host 0.0.0.0 --port 3000

run-dev:
	@trap 'kill 0' EXIT; \
		(cd backend && ./venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload) & \
		(cd frontend && npm run dev -- --host 0.0.0.0 --port 3000) & \
		wait

refresh-cache:
	curl -fsS -X POST http://localhost:8000/api/cache/refresh | python3 -m json.tool

validate-wcm:
	curl -fsS http://localhost:8000/api/process/validate/wcm-context | python3 -m json.tool

verify-backend:
	python3 -m py_compile backend/app/config.py backend/app/main.py backend/app/api/routes_process.py backend/app/services/celonis_service.py backend/app/services/data_cache_service.py backend/app/services/exception_workbench_service.py

build-frontend:
	cd frontend && npm run build

smoke-live:
	./scripts/smoke_live_endpoints.sh http://localhost:8000
