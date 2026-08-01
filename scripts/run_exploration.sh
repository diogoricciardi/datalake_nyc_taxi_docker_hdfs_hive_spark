#!/usr/bin/env bash

set -euo pipefail

spark_master_ctn="spark-master"
spark_master_url="spark://spark-master:7077"
job_path_in_cnt="/jobs/spark/data_exploration.py"

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_file="${project_root}/data/raw/log_hdfs_ingestion.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}

if ! docker ps --format '{{.Names}}' | grep -qx "${spark_master_ctn}"; then
    log "Error: '${spark_master_ctn}' is not running. Start it with docker compose up -d."
    Exit 1
fi

log "Starting job ${job_path_in_cnt}"

docker exec "${spark_master_ctn}" /spark/bin/spark-submit --master "${spark_master_url}" --executor-memory 3g "${job_path_in_cnt}" 2>&1 | tee -a "${log_file}"

log "Job Completed"
