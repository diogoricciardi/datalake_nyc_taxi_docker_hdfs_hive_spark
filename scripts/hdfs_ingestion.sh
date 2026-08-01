#!/usr/bin/env bash

set -euo pipefail

namenode_ctn="namenode"

year="2025"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
host_raw_path="${project_root}/data/raw/${year}"
cnt_raw_path="/data/raw"
hdfs_raw_dir="/data-lake/raw/${year}"
log_file="${project_root}/data/raw/log_hdfs_ingestion.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}

# verifica se o container namenode está rodando
if ! docker ps --format '{{.Names}}' | grep -qx "${namenode_ctn}"; then
    log "Error: '${namenode_ctn}' is not running. Start it with docker compose up -d."
    exit 1
fi

# cria diretórios no HDFS
log "Creating folders in HDFS: '${hdfs_raw_dir}'."
docker exec "${namenode_ctn}" hdfs dfs -mkdir -p "${hdfs_raw_dir}"

# ingestão de 1 put por arquivo, para pegar as falhas de forma individual
failures=()
total_files=0

for local_file in "${host_raw_path}"/*.parquet; do
    filename="$(basename "${local_file}")"
    container_file="${cnt_raw_path}/${filename}"
    destination_hdfs="${hdfs_raw_dir}/${filename}"
    total_files=$((total_files + 1))

    # verifica se arquivo já existe no diretório. se sim, pula para o próximo
    if docker exec "${namenode_ctn}" hdfs dfs -test -e "${destination_hdfs}"; then
        log "${filename} already exists in HDFSs raw dir, skipping."
        continue 
    fi

    # realizando a ingestão do arquivo no hdfs
    log "Ingesting ${filename}."

    if docker exec "${namenode_ctn}" hdfs dfs -put "${container_file}" "${destination_hdfs}"; then
        log "${filename} put complete."
    else
        log "Ingestion failure. ${filename} not put."
        failures+=("${filename}")
    fi
done

# verificando conteúdo do diretório no hdfs
log "Current content in ${hdfs_raw_dir} in HDFS:"
docker exec "${namenode_ctn}" hdfs dfs -ls "${hdfs_raw_dir}" | tee -a "${log_file}"

log "Ingestion completed. ${total_files} files processed."

if [[ ${#failures[@]} -gt 0 ]]; then
    log "Files not put ${failures[*]}"
    exit 1
fi

exit 0

