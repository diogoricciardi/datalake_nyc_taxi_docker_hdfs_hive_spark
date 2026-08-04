#!/usr/bin/env bash
 
set -euo pipefail
 
hive_server_ctn="hive-server"
sql_path_in_cnt="/hive/analytical_queries.hql"
 
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_file="${project_root}/data/raw/log_analytical_queries.log"
 
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}
 
if ! docker ps --format '{{.Names}}' | grep -qx "${hive_server_ctn}"; then
    log "Error '${hive_server_ctn}' is not running. Start it with docker compose up -d."
    exit 1
fi
 
log "Starting analytical queries: ${sql_path_in_cnt}"
 
docker exec "${hive_server_ctn}" beeline \
    -u jdbc:hive2://localhost:10000 \
    -f "${sql_path_in_cnt}" 2>&1 | tee -a "${log_file}"
 
log "Queries completed."