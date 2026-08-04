#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scripts_path="${project_root}/scripts"
log_file="${project_root}/data/raw/log_pipeline.log"
venv_py="${project_root}/.venv/bin/python3"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}

# roda um passo do pipeline e para tudo se ele falhar, informando claramente em qual etapa o problema aconteceu.
run_step() {
    local step_name="$1"
    # remove o primeiro argumento (nome_etapa), sobra só o comando a rodar
    shift

    log "Starting step ${step_name}"

    if "$@"; then
        log "Step completed successfully."
    else
        log "Error: step ${step_name} failed. Pipeline interrupted."
        exit 1
    fi
}

# download dos dados brutos da NYC TLC
run_step "Downloading data." "${scripts_path}/download_data.sh"

# conversão de compressão zstd -> snappy (compatibilidade com o cluster)
run_step "Converting parquet files compression mode." "${venv_py}" "${project_root}/jobs/convert_compression.py"

# ingestão dos dados brutos no HDFS
run_step "HDFS data ingestion." "${scripts_path}/hdfs_ingestion.sh"

# processamento, limpeza e enriquecimento com Spark
run_step "HDFS data ingestion." "${scripts_path}/run_transformation.sh"

# criação da tabela externa no Hive
run_step "HDFS data ingestion." "${scripts_path}/create_hive_tb.sh"

# execução das consultas analíticas
run_step "HDFS data ingestion." "${scripts_path}/run_analytical_queries.sh"

log "Pipeline completed successfully."  