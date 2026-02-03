#!/usr/bin/env bash
set -e

APP_PATH=$1

if [ -z "$APP_PATH" ]; then
  echo "Usage: ./submit.sh <path_to_py_file>"
  exit 1
fi

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  "$APP_PATH"
