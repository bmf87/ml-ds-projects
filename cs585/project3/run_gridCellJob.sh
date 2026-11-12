#!/bin/bash

read -r -p "Enter the number of top cells to report. Default (50): " topK
topK=${topK:-50}

read -r -p "Write results to HDFS (y/n or true/false). Default (n): " write2Hdfs
write2Hdfs=${write2Hdfs:-n}

output_args=()
if [[ $write2Hdfs == 'y' || $write2Hdfs == 'true' ]]; then
  hdfs dfs -mkdir -p /user/cs585/prj_output && \
  hdfs dfs -rm -f /user/cs585/prj_output/grid-cell-top.txt
  output_args=("hdfs:///user/cs585/prj_output/grid-cell-top.txt")
fi

jar_path="$(dirname "${BASH_SOURCE[0]}")/target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar"
if [[ ! -f "$jar_path" ]]; then
  printf 'Missing application JAR. Run sbt package first.\n' >&2
  exit 1
fi

spark-submit \
  --master yarn \
  --deploy-mode client \
  --class dev.bfavro.job.GridCellJob \
  "$jar_path" \
  /user/cs585/prj_input/datasetP.csv \
  "$topK" \
  "${output_args[@]}" \
  "$@"
