#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# DEBUG ONLY (FLINK-41696 investigation, never to be merged).
# Runs the RocksDB memory control job ITERATIONS times on the same machine, each on a fresh
# cluster, and records RocksDB memory metrics plus machine details instead of asserting a limit.

source "$(dirname "$0")"/common.sh

ITERATIONS=${ITERATIONS:-5}
PARALLELISM=2
OUT_DIR="${DEBUG_FILES_OUTPUT_DIR:-$TEST_DATA_DIR}/rocksdb-diag"
mkdir -p "$OUT_DIR"
TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-rocksdb-state-memory-control-test/target/RocksDBStateMemoryControlTestProgram.jar

DIAG_METRICS="block-cache-usage block-cache-pinned-usage block-cache-capacity size-all-mem-tables cur-size-all-mem-tables cur-size-active-mem-table num-immutable-mem-table mem-table-flush-pending num-running-flushes num-running-compactions compaction-pending estimate-table-readers-mem is-write-stopped actual-delayed-write-rate"

function machine_info() {
  echo "### machine"
  echo "nproc: $(nproc 2>/dev/null || sysctl -n hw.ncpu)"
  (lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|Thread|MHz") || sysctl -n machdep.cpu.brand_string
  (free -m 2>/dev/null) || true
  df -h "$TEST_DATA_DIR" | tail -1
  (lsblk -d -o NAME,ROTA,SIZE,MODEL 2>/dev/null) || true
  echo -n "azure vmSize: "
  curl -s -m 2 -H Metadata:true "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text" || echo "n/a"
  echo
  echo "### disk write test (512MB, direct I/O where supported)"
  (dd if=/dev/zero of="$TEST_DATA_DIR/ddtest" bs=1M count=512 oflag=direct 2>&1 \
    || dd if=/dev/zero of="$TEST_DATA_DIR/ddtest" bs=1m count=512 2>&1) | tail -1
  rm -f "$TEST_DATA_DIR/ddtest"
}

metric_name_filter="numRecordsIn"
for m in $DIAG_METRICS; do
  set_config_key "state.backend.rocksdb.metrics.$m" "true"
  metric_name_filter="$metric_name_filter,rocksdb.$m"
done

set_config_key "taskmanager.numberOfTaskSlots" "$PARALLELISM"
set_config_key "taskmanager.memory.process.size" "1184m"
set_config_key "taskmanager.memory.managed.size" "300m"
set_config_key "state.backend.rocksdb.memory.managed" "true"
set_config_key "state.backend.rocksdb.memory.write-buffer-ratio" "0.8"
set_config_key "metrics.fetcher.update-interval" "1000"
setup_flink_slf4j_metric_reporter "$metric_name_filter"

machine_info | tee "$OUT_DIR/machine.txt"

# For each sample, per slot (operator subtask index): shared cache values and sums of the
# per-column-family memtable metrics across all states in that slot.
function tm_logs_oldest_first() {
  # the TM log rotates (.log.2 is older than .log.1, which is older than .log)
  ls "$FLINK_LOG_DIR"/*taskexecutor*.log* \
    | awk '{ n = 0; if (match($0, /\.log\.[0-9]+$/)) n = substr($0, RSTART + 5) + 0; print n "\t" $0 }' \
    | sort -k1,1nr | cut -f2
}

function summarize_iteration() {
  tm_logs_oldest_first | xargs cat | awk '
    /^-- Gauges/ { sample++ }
    /RocksDB test job\..*\.[01]\..*rocksdb[._]/ {
      line = $0
      sub(/.*RocksDB test job\./, "", line)
      split(line, parts, ": ")
      value = parts[2] + 0
      key = parts[1]
      if (match(key, /\.[01]\./) == 0) next
      slot = substr(key, RSTART + 1, 1)
      metric = key
      sub(/.*rocksdb[._]/, "", metric)
      id = sample SUBSEP slot SUBSEP metric
      if (metric ~ /^block-cache/) { v[id] = value } else { v[id] += value }
      samples[sample] = 1
    }
    END {
      printf "%-6s %-4s %9s %9s %9s %9s %6s %6s %6s %6s\n", "sample", "slot", "cacheMB", "pinnedMB", "capMB", "memtblMB", "immut", "flPend", "flRun", "stop"
      for (s = 1; s <= sample; s++) {
        if (!(s in samples)) continue
        for (slot = 0; slot <= 1; slot++) {
          if (!((s SUBSEP slot SUBSEP "block-cache-usage") in v)) continue
          printf "%-6d %-4d %9.1f %9.1f %9.1f %9.1f %6d %6d %6d %6d\n", s, slot,
            v[s, slot, "block-cache-usage"] / 1e6, v[s, slot, "block-cache-pinned-usage"] / 1e6,
            v[s, slot, "block-cache-capacity"] / 1e6, v[s, slot, "size-all-mem-tables"] / 1e6,
            v[s, slot, "num-immutable-mem-table"], v[s, slot, "mem-table-flush-pending"],
            v[s, slot, "num-running-flushes"], v[s, slot, "is-write-stopped"]
        }
      }
    }'
}

# common.sh helpers modify a global "i", so the loop uses its own variable
for iter in $(seq 1 "$ITERATIONS"); do
  echo "### iteration $iter/$ITERATIONS"
  iter_dir="$OUT_DIR/iteration-$iter"
  mkdir -p "$iter_dir/rocksdb-logs"
  set_config_key "state.backend.rocksdb.log.dir" "$iter_dir/rocksdb-logs"
  CHECKPOINT_DIR="$TEST_DATA_DIR/checkpoints-$iter"
  mkdir -p "$CHECKPOINT_DIR"

  start_cluster

  JOB_ID=$("$FLINK_DIR"/bin/flink run -d -p $PARALLELISM "$TEST_PROGRAM_JAR" \
    --environment.parallelism $PARALLELISM \
    --environment.checkpoint_interval 600000 \
    --state_backend rocks \
    --state_backend.checkpoint_directory "file://$CHECKPOINT_DIR" \
    --state_backend.rocks.incremental true \
    --sequence_generator_source.sleep_time 1 \
    --sequence_generator_source.keyspace 1000000 \
    --sequence_generator_source.payload_size 50000 \
    --useValueState true \
    --useListState true \
    --useMapState true \
    --useWindow true \
    --sequence_generator_source.sleep_after_elements 1 | grep "Job has been submitted with JobID" | sed 's/.* //g')

  wait_job_running "$JOB_ID"
  run_start=$(date +%s)
  wait_oper_metric_num_in_records TumblingWindowOperator.0 10000 'RocksDB test job'
  run_seconds=$(( $(date +%s) - run_start ))
  cancel_job "$JOB_ID"
  stop_cluster

  summarize_iteration | tee "$iter_dir/summary.txt"
  peaks=$(awk 'NR > 1 { if ($3 > m[$2]) m[$2] = $3; if ($6 > t[$2]) t[$2] = $6; if ($7 > q[$2]) q[$2] = $7 }
    END { printf "slot0=%.1fMB slot1=%.1fMB maxMemtables=%.1f/%.1fMB maxImmutable=%d/%d", m[0], m[1], t[0], t[1], q[0], q[1] }' "$iter_dir/summary.txt")
  echo "RESULT iteration=$iter time10k=${run_seconds}s $peaks" | tee -a "$OUT_DIR/results.txt"

  mkdir -p "$iter_dir/flink-logs"
  mv "$FLINK_LOG_DIR"/* "$iter_dir/flink-logs/" 2>/dev/null
  rm -rf "$CHECKPOINT_DIR"
done

echo "### all iterations"
cat "$OUT_DIR/results.txt"
