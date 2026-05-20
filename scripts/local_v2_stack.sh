#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${EDU_CTI_LOCAL_ENV_FILE:-$ROOT_DIR/.env.local}"
BASE_ENV_FILE="$ROOT_DIR/.env"
COMPOSE_FILE="$ROOT_DIR/docker-compose.local.yml"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
DASHBOARD_DIR="${EDU_CTI_DASHBOARD_DIR:-$ROOT_DIR/../EduThreat-CTI-Dashboard}"

usage() {
  cat <<'EOF'
Usage: scripts/local_v2_stack.sh <command>

Commands:
  init-env        Create .env.local and dashboard .env.local if missing.
  install-deps    Install Python package/deps plus Playwright/Patchright browsers.
  db-up           Start persistent local Postgres in Docker.
  db-down         Stop local Postgres without deleting the volume.
  db-logs         Tail local Postgres logs.
  migrate         Run Alembic migrations against local Postgres.
  preflight       Run v2 preflight checks.
  api             Start local v2 API at http://127.0.0.1:${PORT:-8000}.
  worker          Start local v2 worker runtime.
  run-plan [name] Collect/enqueue a named plan without draining tasks.
  status          Print API health and admin status URLs to check.

Typical local soak:
  scripts/local_v2_stack.sh init-env
  scripts/local_v2_stack.sh install-deps
  scripts/local_v2_stack.sh db-up
  scripts/local_v2_stack.sh migrate
  scripts/local_v2_stack.sh api
  scripts/local_v2_stack.sh worker
  scripts/local_v2_stack.sh run-plan rss_fast_refresh
EOF
}

ensure_env_file() {
  if [[ ! -f "$ENV_FILE" ]]; then
    cp "$ROOT_DIR/.env.local.example" "$ENV_FILE"
    echo "Created $ENV_FILE"
  fi
}

ensure_compose_file() {
  if [[ -f "$COMPOSE_FILE" ]]; then
    return
  fi

  cat > "$COMPOSE_FILE" <<'EOF'
name: eduthreat-local

services:
  postgres:
    image: postgres:16-alpine
    container_name: eduthreat-v2-postgres-local
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${EDU_CTI_LOCAL_POSTGRES_DB:-eduthreat_cti_v2}
      POSTGRES_USER: ${EDU_CTI_LOCAL_POSTGRES_USER:-eduthreat}
      POSTGRES_PASSWORD: ${EDU_CTI_LOCAL_POSTGRES_PASSWORD:-eduthreat_local_password}
    ports:
      - "${EDU_CTI_LOCAL_POSTGRES_PORT:-55433}:5432"
    volumes:
      - eduthreat_v2_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 3s
      retries: 20

volumes:
  eduthreat_v2_postgres_data:
EOF
  echo "Created $COMPOSE_FILE"
}

load_env() {
  ensure_env_file
  set -a
  # Load secrets first, then local overrides. This keeps existing .env secrets
  # usable without copying them into .env.local.
  if [[ -f "$BASE_ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$BASE_ENV_FILE"
  fi
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
}

compose() {
  ensure_env_file
  ensure_compose_file
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

ensure_python() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Python virtualenv not found at $PYTHON_BIN"
    echo "Create it first, for example: python3 -m venv .venv"
    exit 1
  fi
}

wait_for_db() {
  echo "Waiting for local Postgres..."
  for _ in {1..60}; do
    if compose exec -T postgres pg_isready -U "${EDU_CTI_LOCAL_POSTGRES_USER:-eduthreat}" -d "${EDU_CTI_LOCAL_POSTGRES_DB:-eduthreat_cti_v2}" >/dev/null 2>&1; then
      echo "Local Postgres is ready."
      return 0
    fi
    sleep 1
  done
  echo "Local Postgres did not become ready in time."
  compose ps
  exit 1
}

init_env() {
  ensure_env_file
  if [[ -d "$DASHBOARD_DIR" && ! -f "$DASHBOARD_DIR/.env.local" ]]; then
    cat > "$DASHBOARD_DIR/.env.local" <<'EOF'
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_SITE_NAME=EduThreat-CTI
NEXT_PUBLIC_SITE_DESCRIPTION=Cyber Threat Intelligence for Education Sector
EOF
    echo "Created $DASHBOARD_DIR/.env.local"
  elif [[ -d "$DASHBOARD_DIR" ]]; then
    echo "Dashboard env already exists: $DASHBOARD_DIR/.env.local"
  else
    echo "Dashboard directory not found at $DASHBOARD_DIR; skipping dashboard env."
  fi
}

install_deps() {
  ensure_python
  "$PYTHON_BIN" -m pip install -r "$ROOT_DIR/requirements.txt"
  "$PYTHON_BIN" -m pip install -e "$ROOT_DIR"
  "$PYTHON_BIN" -m playwright install chromium
  "$ROOT_DIR/.venv/bin/patchright" install chromium
}

run_api() {
  load_env
  ensure_python
  cd "$ROOT_DIR"
  exec "$PYTHON_BIN" -m src.edu_cti_v2.api_server --host 127.0.0.1 --port "${PORT:-8000}" --reload
}

run_worker() {
  load_env
  ensure_python
  cd "$ROOT_DIR"
  args=(
    -m src.edu_cti_v2.runtime
    --workers "${EDU_CTI_V2_WORKER_COUNT:-4}"
    --fetch-workers "${EDU_CTI_V2_FETCH_WORKER_COUNT:-2}"
    --resolve-workers "${EDU_CTI_V2_RESOLVE_WORKER_COUNT:-1}"
    --canonicalize-workers "${EDU_CTI_V2_CANONICALIZE_WORKER_COUNT:-1}"
  )
  if [[ "${EDU_CTI_V2_ENABLE_SCHEDULER:-0}" == "0" ]]; then
    args+=(--no-scheduler)
  fi
  if [[ "${EDU_CTI_V2_PREWARM_MODELS:-0}" == "0" ]]; then
    args+=(--no-prewarm-models)
  fi
  exec "$PYTHON_BIN" "${args[@]}"
}

run_plan() {
  load_env
  ensure_python
  local plan="${1:-rss_fast_refresh}"
  cd "$ROOT_DIR"
  "$PYTHON_BIN" -m src.edu_cti_v2.orchestrator_cli "$plan" --no-drain --exclude-paid-rss
}

case "${1:-}" in
  init-env)
    init_env
    ;;
  install-deps)
    install_deps
    ;;
  db-up)
    load_env
    compose up -d postgres
    wait_for_db
    ;;
  db-down)
    compose down
    ;;
  db-logs)
    compose logs -f postgres
    ;;
  migrate)
    load_env
    ensure_python
    cd "$ROOT_DIR"
    "$PYTHON_BIN" -m src.edu_cti_v2.migrate upgrade head
    ;;
  preflight)
    load_env
    ensure_python
    cd "$ROOT_DIR"
    "$PYTHON_BIN" -m src.edu_cti_v2.preflight_cli
    ;;
  api)
    run_api
    ;;
  worker)
    run_worker
    ;;
  run-plan)
    shift || true
    run_plan "${1:-rss_fast_refresh}"
    ;;
  status)
    load_env
    echo "API health:        http://127.0.0.1:${PORT:-8000}/api/health"
    echo "API docs:          http://127.0.0.1:${PORT:-8000}/docs"
    echo "Dashboard:         http://localhost:3000"
    echo "Admin login:       ${EDUTHREAT_ADMIN_USERNAME:-admin} / ${EDUTHREAT_ADMIN_PASSWORD:-admin123}"
    echo "Admin API key:     ${EDUTHREAT_ADMIN_API_KEY:-local-dev-admin-key}"
    echo "Postgres volume:   eduthreat_v2_postgres_data"
    ;;
  *)
    usage
    exit 1
    ;;
esac
