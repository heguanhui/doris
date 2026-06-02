#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to generate TPC-H data set with multi-disk
# parallel support. Use -d to specify comma-separated data
# directories on different disks for maximum I/O throughput.
#
# If -d is not specified, it falls back to the original
# single-directory (tpch-data/) behavior, identical to
# gen-tpch-data.sh.
#
# After generation, a data-manifest.json is written to ${CURDIR}/meta/,
# recording data_dirs, scale_factor, and the complete file list per table
# for load script to consume.
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCH_DBGEN_DIR="${CURDIR}/TPC-H_Tools_v3.0.0/dbgen/"
TPCH_DATA_DIR="${CURDIR}/tpch-data/"

MANIFEST_NAME="data-manifest.json"
META_DIR="${CURDIR}/meta"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     -s             scale factor, default is 100
     -c             parallelism to generate data of (lineitem, orders, partsupp) table per disk, default is 10
     -d             comma-separated data directories on different disks for parallel generation.
                    If not specified, falls back to single directory (tpch-data/) behavior.
                    Each path must reside on a different disk. Total chunks = parallelism * number_of_disks,
                    each disk handles 'parallelism' chunks.

  Eg.
    $0 -s 100 -c 10                                                    generate data using default single directory.
    $0 -s 100 -c 5 -d /data1/tpch,/data2/tpch,/data3/tpch              generate data across 3 disks, 5 chunks per disk (15 total).
    $0 -s 1000 -c 10 -d /data1/tpch,/data2/tpch,/data3/tpch,/data4/tpch  generate SF=1000 data across 4 disks, 10 chunks per disk (40 total).
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hs:c:d:' \
    -- "$@")

eval set -- "${OPTS}"

SCALE_FACTOR=100
PARALLEL=10
DATA_DIRS=""
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -s)
        SCALE_FACTOR=$2
        shift 2
        ;;
    -c)
        PARALLEL=$2
        shift 2
        ;;
    -d)
        DATA_DIRS=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Scale Factor: ${SCALE_FACTOR}"
echo "Parallelism per disk: ${PARALLEL}"

# check if dbgen exists
if [[ ! -f ${TPCH_DBGEN_DIR}/dbgen ]]; then
    echo "${TPCH_DBGEN_DIR}/dbgen does not exist. Run build-tpch-dbgen.sh first to build it first."
    exit 1
fi

# Parse DATA_DIRS from -d parameter
DATA_DIR_ARRAY=()
if [[ -n "${DATA_DIRS}" ]]; then
    IFS=',' read -ra DATA_DIR_ARRAY <<< "${DATA_DIRS}"
fi

NUM_DISKS=${#DATA_DIR_ARRAY[@]}

# Helper: write manifest JSON to ${META_DIR}/
# Usage: write_manifest <scale_factor> <data_dirs_json_array> <parallel> <num_disks>
write_manifest() {
    local sf=$1
    local dirs_json=$2
    local parallel=$3
    local num_disks=$4

    mkdir -p "${META_DIR}"
    local manifest_path="${META_DIR}/${MANIFEST_NAME}"

    # Build data_file_distribution per table
    local dist_json="{"
    local first_table=1

    for table_name in region nation supplier customer part partsupp orders lineitem; do
        if [[ ${first_table} -eq 0 ]]; then
            dist_json+=","
        fi
        first_table=0

        local shard=1
        if [[ "${table_name}" == "partsupp" || "${table_name}" == "orders" || "${table_name}" == "lineitem" ]]; then
            shard=$((parallel * num_disks))
        fi

        # Collect files for this table across all data directories
        local files_json="["
        local first_file=1
        for dir in "${DATA_DIR_ARRAY[@]}"; do
            while IFS= read -r -d '' f; do
                if [[ ${first_file} -eq 0 ]]; then
                    files_json+=","
                fi
                first_file=0
                files_json+="\"${f}\""
            done < <(find "${dir}" -maxdepth 1 -name "${table_name}.tbl*" -print0 2>/dev/null | sort -z)
        done
        files_json+="]"

        dist_json+="\"${table_name}\":{\"shard\":${shard},\"data_files\":${files_json}}"
    done
    dist_json+="}"

    cat > "${manifest_path}" <<MANIFEST_EOF
{
  "total_table_num": 8,
  "data_dirs": ${dirs_json},
  "scale_factor": ${sf},
  "data_file_distribution": ${dist_json}
}
MANIFEST_EOF

    echo "Manifest written to ${manifest_path}"
}

if [[ ${NUM_DISKS} -eq 0 ]]; then
    # Single-disk mode: original behavior (identical to gen-tpch-data.sh)
    echo "Mode: single-disk (using default ${TPCH_DATA_DIR})"
    TOTAL_CHUNKS=${PARALLEL}

    if [[ -d ${TPCH_DATA_DIR}/ ]]; then
        echo "${TPCH_DATA_DIR} exists. Remove it before generating data"
        exit 1
    fi
    mkdir "${TPCH_DATA_DIR}"/

    # Clean up any stale .tbl* files from previous runs in dbgen directory
    rm -f "${TPCH_DBGEN_DIR}"/*.tbl*

    # gen data
    cd "${TPCH_DBGEN_DIR}"
    echo "Begin to generate data for table: region"
    "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T r
    echo "Begin to generate data for table: nation"
    "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T n
    echo "Begin to generate data for table: supplier"
    "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T s
    echo "Begin to generate data for table: part"
    "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T P
    echo "Begin to generate data for table: customer"
    "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T c
    echo "Begin to generate data for table: partsupp"
    for i in $(seq 1 "${PARALLEL}"); do
        {
            "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T S -C "${PARALLEL}" -S "${i}"
        } &
    done
    wait

    echo "Begin to generate data for table: orders"
    for i in $(seq 1 "${PARALLEL}"); do
        {
            "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T O -C "${PARALLEL}" -S "${i}"
        } &
    done
    wait

    echo "Begin to generate data for table: lineitem"
    for i in $(seq 1 "${PARALLEL}"); do
        {
            "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T L -C "${PARALLEL}" -S "${i}"
        } &
    done
    wait

    cd -

    # move data to ${TPCH_DATA_DIR}
    mv "${TPCH_DBGEN_DIR}"/*.tbl* "${TPCH_DATA_DIR}"/

    # check data
    du -sh "${TPCH_DATA_DIR}"/*.tbl*

    # Write manifest for single-disk mode
    TPCH_DATA_DIR_NO_SLASH="${TPCH_DATA_DIR%/}"
    DIRS_JSON="[\"${TPCH_DATA_DIR_NO_SLASH}\"]"
    DATA_DIR_ARRAY=("${TPCH_DATA_DIR_NO_SLASH}")
    write_manifest "${SCALE_FACTOR}" "${DIRS_JSON}" "${PARALLEL}" 1
else
    # Multi-disk mode
    echo "Mode: multi-disk (${NUM_DISKS} disks, ${PARALLEL} parallelism per disk)"

    # Validate that each DATA_DIR is on a different disk
    declare -A DISK_MAP
    for dir in "${DATA_DIR_ARRAY[@]}"; do
        check_dir="${dir}"
        if [[ ! -d "${check_dir}" ]]; then
            check_dir=$(dirname "${check_dir}")
        fi
        if [[ ! -d "${check_dir}" ]]; then
            echo "Error: Neither ${dir} nor its parent directory exists."
            exit 1
        fi
        fs_device=$(df "${check_dir}" | tail -1 | awk '{print $1}')
        if [[ -n "${DISK_MAP[${fs_device}]}" ]]; then
            echo "Error: ${dir} and ${DISK_MAP[${fs_device}]} are on the same disk (${fs_device})."
            echo "Each data directory must reside on a different disk for parallel I/O benefit."
            exit 1
        fi
        DISK_MAP[${fs_device}]="${dir}"
    done
    echo "Disk validation passed: all ${NUM_DISKS} paths are on different disks."

    # Check and create data directories
    for dir in "${DATA_DIR_ARRAY[@]}"; do
        if [[ -d "${dir}" ]]; then
            echo "${dir} exists. Remove it before generating data"
            exit 1
        fi
        mkdir -p "${dir}"
    done

    TOTAL_SHARDS=$((PARALLEL * NUM_DISKS))
    echo "Total shards per large table: ${TOTAL_SHARDS} (${PARALLEL} per disk × ${NUM_DISKS} disks)"

    # Estimate required disk space per disk (rough estimate in GB)
    # TPC-H scale factor roughly equals GB, with some overhead for text format (~1.5x-2x)
    ESTIMATED_GB_PER_DISK=$((SCALE_FACTOR * 2 / NUM_DISKS))
    echo "Estimated space needed per disk: ~${ESTIMATED_GB_PER_DISK} GB"

    # Check available disk space for each directory
    for dir in "${DATA_DIR_ARRAY[@]}"; do
        avail_kb=$(df -k "${dir}" | tail -1 | awk '{print $4}')
        avail_gb=$((avail_kb / 1024 / 1024))
        if [[ ${avail_gb} -lt ${ESTIMATED_GB_PER_DISK} ]]; then
            echo "Error: ${dir} has only ${avail_gb} GB available, but estimated ${ESTIMATED_GB_PER_DISK} GB needed."
            exit 1
        fi
        echo "Disk space check passed for ${dir}: ${avail_gb} GB available"
    done

    FIRST_DIR="${DATA_DIR_ARRAY[0]}"
    PROGRESS_LOG_INTERVAL=10
    ESTIMATED_TOTAL_BYTES=$((SCALE_FACTOR * 2 * 1024 * 1024 * 1024))

    progress_monitor() {
        local pids=("$@")
        local prev_bytes=0
        local prev_epoch=$(date +%s)
        local start_epoch=${prev_epoch}

        while true; do
            sleep ${PROGRESS_LOG_INTERVAL}

            local any_alive=0
            for pid in "${pids[@]}"; do
                if [[ -d /proc/${pid} ]]; then
                    any_alive=1
                    break
                fi
            done

            local now_epoch=$(date +%s)
            local total_bytes=0
            for d in "${DATA_DIR_ARRAY[@]}"; do
                if [[ -d "${d}" ]]; then
                    local sz=$(find "${d}" -maxdepth 1 -name '*.tbl*' -exec stat --format='%s' {} + 2>/dev/null | awk '{s+=$1}END{print s+0}')
                    total_bytes=$((total_bytes + sz))
                fi
            done

            local interval_bytes=$((total_bytes - prev_bytes))
            local interval_secs=$((now_epoch - prev_epoch))
            if [[ ${interval_secs} -le 0 ]]; then
                interval_secs=1
            fi
            local total_elapsed=$((now_epoch - start_epoch))
            if [[ ${total_elapsed} -le 0 ]]; then
                total_elapsed=1
            fi
            local current_speed=0
            local avg_speed=0
            if [[ ${interval_bytes} -gt 0 ]]; then
                current_speed=$((interval_bytes / interval_secs))
            fi
            if [[ ${total_bytes} -gt 0 ]]; then
                avg_speed=$((total_bytes / total_elapsed))
            fi

            local total_mb=$((total_bytes / 1024 / 1024))
            local current_speed_mb=$(awk "BEGIN{printf \"%.2f\", ${current_speed}/1024/1024}")
            local avg_speed_mb=$(awk "BEGIN{printf \"%.2f\", ${avg_speed}/1024/1024}")

            local eta_str="--"
            if [[ ${avg_speed} -gt 0 && ${ESTIMATED_TOTAL_BYTES} -gt ${total_bytes} ]]; then
                local remaining_bytes=$((ESTIMATED_TOTAL_BYTES - total_bytes))
                local eta_secs=$((remaining_bytes / avg_speed))
                local eta_m=$((eta_secs / 60))
                local eta_s=$((eta_secs % 60))
                eta_str="${eta_m}m${eta_s}s"
            fi

            echo "[Progress] $(date '+%Y-%m-%d %H:%M:%S') total: ${total_mb}MB | current speed: ${current_speed_mb}MB/s | avg speed: ${avg_speed_mb}MB/s | elapsed: ${total_elapsed}s | ETA: ${eta_str}"

            prev_bytes=${total_bytes}
            prev_epoch=${now_epoch}

            if [[ ${any_alive} -eq 0 ]]; then
                break
            fi
        done
    }

    # Launch all generation tasks in parallel (small tables + large table shards)
    # dbgen outputs to the current working directory, so we cd to the target data dir
    # before running dbgen. No mv needed — files are generated directly in place.
    # All tasks run in background, one wait at the end.
    # Collect all dbgen PIDs so we can wait only on them (not the monitor).

    DBGEN_PIDS=()
    echo "Launching all generation tasks in parallel..."

    # Small tables (region, nation, supplier, part, customer) — single shard, output to first dir
    echo "[region] Generating -> ${FIRST_DIR}"
    (cd "${FIRST_DIR}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T r) &
    DBGEN_PIDS+=($!)

    echo "[nation] Generating -> ${FIRST_DIR}"
    (cd "${FIRST_DIR}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T n) &
    DBGEN_PIDS+=($!)

    echo "[supplier] Generating -> ${FIRST_DIR}"
    (cd "${FIRST_DIR}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T s) &
    DBGEN_PIDS+=($!)

    echo "[part] Generating -> ${FIRST_DIR}"
    (cd "${FIRST_DIR}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T P) &
    DBGEN_PIDS+=($!)

    echo "[customer] Generating -> ${FIRST_DIR}"
    (cd "${FIRST_DIR}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T c) &
    DBGEN_PIDS+=($!)

    # Large tables — each shard assigned to a disk dir, all shards run in parallel
    # Shard assignment: disk 0 gets shards 1..PARALLEL, disk 1 gets shards (PARALLEL+1)..(2*PARALLEL), etc.
    for disk_idx in $(seq 0 $((NUM_DISKS - 1))); do
        dir="${DATA_DIR_ARRAY[${disk_idx}]}"
        start_shard=$((disk_idx * PARALLEL + 1))
        end_shard=$(((disk_idx + 1) * PARALLEL))

        for shard in $(seq ${start_shard} ${end_shard}); do
            echo "[partsupp] Shard ${shard} -> ${dir}"
            (cd "${dir}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T S -C "${TOTAL_SHARDS}" -S "${shard}") &
            DBGEN_PIDS+=($!)

            echo "[orders] Shard ${shard} -> ${dir}"
            (cd "${dir}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T O -C "${TOTAL_SHARDS}" -S "${shard}") &
            DBGEN_PIDS+=($!)

            echo "[lineitem] Shard ${shard} -> ${dir}"
            (cd "${dir}" && DSS_CONFIG="${TPCH_DBGEN_DIR}" "${TPCH_DBGEN_DIR}"/dbgen -f -s "${SCALE_FACTOR}" -T L -C "${TOTAL_SHARDS}" -S "${shard}") &
            DBGEN_PIDS+=($!)
        done
    done

    # Start progress monitor — it exits when all dbgen PIDs are gone
    progress_monitor "${DBGEN_PIDS[@]}" &
    MONITOR_PID=$!

    echo "Waiting for all generation tasks to complete..."
    FAILED_PIDS=()
    for pid in "${DBGEN_PIDS[@]}"; do
        if ! wait "${pid}" 2>/dev/null; then
            FAILED_PIDS+=("${pid}")
        fi
    done
    echo "All generation tasks completed."
    if [[ ${#FAILED_PIDS[@]} -gt 0 ]]; then
        echo "Error: ${#FAILED_PIDS[@]} generation task(s) failed: ${FAILED_PIDS[*]}"
        exit 1
    fi

    # Wait for monitor to flush last line then kill it
    sleep 2
    kill ${MONITOR_PID} 2>/dev/null || true
    wait ${MONITOR_PID} 2>/dev/null || true

    # Check data across all directories
    echo "===== Data generation complete. Summary: ====="
    for dir in "${DATA_DIR_ARRAY[@]}"; do
        echo "--- ${dir} ---"
        du -sh "${dir}"/*.tbl* 2>/dev/null || echo "(no files)"
    done

    # Write manifest for multi-disk mode
    DIRS_JSON="["
    for i in "${!DATA_DIR_ARRAY[@]}"; do
        if [[ ${i} -gt 0 ]]; then
            DIRS_JSON+=","
        fi
        DIRS_JSON+="\"${DATA_DIR_ARRAY[${i}]}\""
    done
    DIRS_JSON+="]"
    write_manifest "${SCALE_FACTOR}" "${DIRS_JSON}" "${PARALLEL}" "${NUM_DISKS}"
fi
