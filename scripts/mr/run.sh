#!/usr/bin/env bash
# Objective 4 — MapReduce abnormal-event frequency over raw ICU records (HDFS -> YARN).
# Usage: bash scripts/mr/run.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HOME/hadoop/conf}"

RAW="/user/alvi/vytals/raw/physionet2012/set-a"
ALL="/user/alvi/vytals/raw/physionet2012/set-a_all.txt"
OUT="/user/alvi/vytals/mr/abnormal_freq"

# 4000 tiny per-patient files -> 1 split would still be 4000 map tasks (one JVM
# container each, ~8x slower on this lab box). Merge into one 26 MB file first:
# the mapper is line-oriented, so concatenated records are safe.
if ! hdfs dfs -test -s "$ALL" 2>/dev/null; then
  echo "merging 4000 records -> $ALL"
  hdfs dfs -cat "$RAW/*.txt" | hdfs dfs -put - "$ALL"
fi

hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true

hadoop jar /usr/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
  -files scripts/mr/abnormal_mapper.py,scripts/mr/abnormal_reducer.py \
  -Dmapreduce.map.memory.mb=512 -Dmapreduce.reduce.memory.mb=512 \
  -Dmapreduce.map.java.opts=-Xmx384m -Dmapreduce.reduce.java.opts=-Xmx384m \
  -mapper abnormal_mapper.py -reducer abnormal_reducer.py \
  -input "$ALL" \
  -output "$OUT"

echo "=== abnormal-event frequency (param, total, abnormal, pct) ==="
hdfs dfs -cat "$OUT"/part-*
