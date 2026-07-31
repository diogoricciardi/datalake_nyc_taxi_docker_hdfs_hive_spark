#!/usr/bin/env bash

set -euo pipefail

# definindo parâmetros de seleção do recorte de dados
taxi_type="yellow"
year="2025"
base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"

# dinamiza a localização do diretório data/raw e do arquivo de log
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
raw_dir="${project_root}/data/raw/${year}"
log_file="${project_root}/data/raw/download_data.log"

# cria o diretório data/raw se ainda não existir
mkdir -p "${raw_dir}"

# comando log() define log como uma função
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${log_file}"
}

log "Starting download of NYC TLC record data file. Taxi Type=${taxi_type} Year=${year}" 
log "Destination: ${raw_dir}"

# loop para baixar os arquivos parquet de cada mês
failures=() # array
for month in $(seq -w 1 12); do
    filename="${taxi_type}_tripdata_${year}-${month}.parquet"
    url="${base_url}/${filename}"
    destination="${raw_dir}/${filename}"

    # valida se o arquivo já existe. garante idempotência do código
    if [[ -f "${destination}" ]]; then
        log "File already exists. Skipping ${filename}"
        continue
    fi

    log "Downloading ${filename}"

    # se o arquivo foi baixado com sucesso, loga o nome e o tamanho senão loga a falha, remove qq arquivo parcial criado e adiciona o nome do arquivo
    # na array de falhas
    if curl -fSL --retry 3 --retry-delay 5 -o "${destination}" "${url}"; then
        file_size=$(du -h "${destination}" | cut -f1)
        log "OK: ${filename} ${file_size}"
    else
        log "Failure downloading ${filename}"
        rm -f "${destination}"
        failures+=("${filename}")
    fi
done

total_number_of_files=$(find "${raw_dir}" -name "*.parquet" | wc -l)
log "Download completed. ${total_number_of_files}/12 files available in ${raw_dir}."

# se a quantidade de arquivos com falha for maior q zero retorna 
if [[ ${#failures[@]} -gt 0 ]]; then
    log "Failed downloas ${failures[*]}"
    exit 1
fi

exit 0

