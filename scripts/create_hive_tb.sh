#!/usr/bin/env bash

set -euo pipefail

hive_server_ctn="hive-server"
sql_path_in_ctn="/hive/create_external_tb.hql"

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_file="${project_root}/data/raw/log_create_hive_tb.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}

if ! docker ps --format '{{.Names}}' | grep -qx "${hive_server_ctn}"; then
    log "Error '${hive_server_ctn}' is not running. Start it with docker compose up -d."
    exit 1
fi

log "Creating external table through: ${sql_path_in_ctn}"

docker exec "${hive_server_ctn}" beeline -u jdbc:hive2://localhost:10000 -f "${sql_path_in_ctn}" 2>&1 | tee -a "${log_file}"

log "Table creation completed."