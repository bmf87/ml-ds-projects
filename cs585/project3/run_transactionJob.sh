#!/bin/bash

read -r -p "Enter the preview limit. Default (20): " maxPreview
maxPreview=${maxPreview:-20}
#echo -e "Running TransactionJob with preview limit $maxPreview"

read -r -p "Write results to HDFS (y/n or true/false). Default (n): " write2Hdfs
write2Hdfs=${write2Hdfs:-n}
#echo -e "Writing results to HDFS: $write2Hdfs\n"

output_args=()
debug_args=()
debugSteps=false

if [[ $write2Hdfs == 'y' || $write2Hdfs == 'true' ]]; then
  hdfs dfs -mkdir -p /user/cs585/prj_output && \
  hdfs dfs -rm -f /user/cs585/prj_output/transaction-steps.txt
  output_args=("hdfs:///user/cs585/prj_output/transaction-steps.txt")
  
  read -r -p "Write debug results to HDFS (y/n or true/false). Default (n): " write2DebugHdfs
  write2DebugHdfs=${write2DebugHdfs:-n}
  #echo -e "Writing debug results to HDFS: $write2DebugHdfs\n"
  if [[ $write2DebugHdfs == 'y' || $write2DebugHdfs == 'true' ]]; then
    debugSteps=true
    hdfs dfs -rm -f /user/cs585/prj_output/transaction-debug-steps.txt
    DEBUG_OUTPUT_PATH="hdfs:///user/cs585/prj_output/transaction-debug-steps.txt" 
    debug_args=(--conf "spark.transactionJob.debugOutputPath=$DEBUG_OUTPUT_PATH")
  fi
fi

jar_path="$(dirname "${BASH_SOURCE[0]}")/target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar"
if [[ ! -f "$jar_path" ]]; then
  printf 'Missing application JAR. Run sbt package first.\n' >&2
  exit 1
fi

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf "spark.transactionJob.debugSteps=$debugSteps" \
  --conf "spark.transactionJob.multiplier=${T6_MULTIPLIER:-3}" \
  "${debug_args[@]}" \
  --class dev.bfavro.job.TransactionJob \
  "$jar_path" \
  /user/cs585/prj_input/transactions.csv \
  "$maxPreview" \
  "${output_args[@]}" \
  "$@"