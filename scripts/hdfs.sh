#!/usr/bin/env bash
# Vytals — single-node HDFS lifecycle (Hadoop 3.4.x, JDK 17).
#
#   scripts/hdfs.sh up       format (first run only) + start NameNode/DataNode
#   scripts/hdfs.sh down     stop daemons
#   scripts/hdfs.sh status   daemon + cluster summary
#   scripts/hdfs.sh upload   push PhysioNet Challenge 2012 + CSVs into HDFS
#   scripts/hdfs.sh tree     show the vytals HDFS layout
#
# Config lives in ~/hadoop/conf (core-site.xml: fs.defaultFS=hdfs://localhost:9000,
# hdfs-site.xml: replication=1, NN/DN dirs under ~/hadoop/tmp/dfs).
set -euo pipefail

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HOME/hadoop/conf}"
export HADOOP_LOG_DIR="${HADOOP_LOG_DIR:-$HOME/hadoop/logs}"
export HADOOP_PID_DIR="${HADOOP_PID_DIR:-$HOME/hadoop/pids}"

# Spark 3.5 + Hadoop 3.4 need 8/11/17 — pin 17 if present, else whatever works.
pick_java() {
  local c
  for c in /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/java-11-openjdk /usr/lib/jvm/java-8-openjdk; do
    if [ -x "$c/bin/java" ]; then echo "$c"; return; fi
  done
  echo "ERROR: no JDK 8/11/17 in /usr/lib/jvm (install jdk17-openjdk)" >&2
  exit 1
}
export JAVA_HOME="$(pick_java)"

HDFS_ROOT="${HDFS_ROOT:-/user/alvi/vytals}"
LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PN="$LOCAL_ROOT/data/physionet2012"

cmd="${1:-status}"

daemons_up() {
  pgrep -f "proc_namenode" >/dev/null 2>&1 && pgrep -f "proc_datanode" >/dev/null 2>&1
}

yarn_up() {
  pgrep -f "proc_resourcemanager" >/dev/null 2>&1 && pgrep -f "proc_nodemanager" >/dev/null 2>&1
}

do_up() {
  echo "JAVA_HOME=$JAVA_HOME  HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
  # stale pid files block startup
  find "$HADOOP_PID_DIR" -name '*.pid' 2>/dev/null | while read -r f; do
    p=$(cat "$f" 2>/dev/null || true)
    { [ -n "$p" ] && kill -0 "$p" 2>/dev/null; } || rm -f "$f"
  done

  if [ ! -f "$HOME/hadoop/tmp/dfs/name/current/VERSION" ]; then
    echo "==> formatting NameNode (first run)"
    hdfs namenode -format -force -nonInteractive >/dev/null
  fi

  if daemons_up; then
    echo "==> HDFS daemons already running"
  else
    echo "==> starting NameNode + DataNode"
    hdfs --daemon start namenode
    sleep 3
    hdfs --daemon start datanode
    sleep 3
  fi
  if yarn_up; then
    echo "==> YARN daemons already running"
  else
    echo "==> starting ResourceManager + NodeManager (MapReduce needs YARN)"
    yarn --daemon start resourcemanager
    sleep 2
    yarn --daemon start nodemanager
    sleep 3
  fi
  do_status
}

do_down() {
  echo "==> stopping HDFS + YARN daemons"
  yarn --daemon stop nodemanager 2>/dev/null || true
  yarn --daemon stop resourcemanager 2>/dev/null || true
  hdfs --daemon stop datanode 2>/dev/null || true
  hdfs --daemon stop namenode 2>/dev/null || true
  echo "stopped"
}

do_status() {
  if ! daemons_up; then
    echo "HDFS: DOWN (run: scripts/hdfs.sh up)"
    return 1
  fi
  echo "HDFS: UP"
  hdfs dfsadmin -report 2>/dev/null | grep -E "Configured Capacity|DFS Remaining:|DFS Used:|Live datanodes"
  echo "safemode: $(hdfs dfsadmin -safemode get 2>/dev/null | tail -1)"
  if yarn_up; then echo "YARN: UP"; else echo "YARN: DOWN (MapReduce will fail — run: scripts/hdfs.sh up)"; fi
}

do_upload() {
  daemons_up || { echo "HDFS down — run scripts/hdfs.sh up first" >&2; exit 1; }
  [ -d "$PN/set-a" ] || { echo "missing $PN/set-a — run: make real-data (or see README)" >&2; exit 1; }

  echo "==> $HDFS_ROOT/{raw/physionet2012,csv}"
  hdfs dfs -mkdir -p "$HDFS_ROOT/raw/physionet2012" "$HDFS_ROOT/csv"

  echo "==> raw ICU records (set-a, 4000 files, parallel put)"
  hdfs dfs -put -f -t 8 "$PN/set-a" "$HDFS_ROOT/raw/physionet2012/"

  echo "==> outcomes + archive + pipeline CSVs"
  hdfs dfs -put -f "$PN/Outcomes-a.txt" "$PN/set-a.zip" "$HDFS_ROOT/raw/physionet2012/"
  hdfs dfs -put -f "$LOCAL_ROOT/data/vitals.csv" "$LOCAL_ROOT/data/vitals_real.csv" "$HDFS_ROOT/csv/" 2>/dev/null \
    || echo "(csv/ skipped — generate with make sample / make real-data)"

  do_tree
}

do_tree() {
  echo "==> HDFS layout"
  hdfs dfs -ls "$HDFS_ROOT/raw/physionet2012" "$HDFS_ROOT/csv" 2>/dev/null
  echo -n "set-a records in HDFS: "
  hdfs dfs -ls "$HDFS_ROOT/raw/physionet2012/set-a" 2>/dev/null | grep -c '\.txt$' || true
  echo -n "total size: "
  hdfs dfs -du -s -h "$HDFS_ROOT" 2>/dev/null | awk '{print $1, $2}'
}

case "$cmd" in
  up)     do_up ;;
  down)   do_down ;;
  status) do_status ;;
  upload) do_upload ;;
  tree)   do_tree ;;
  *)      echo "usage: $0 {up|down|status|upload|tree}" >&2; exit 2 ;;
esac
