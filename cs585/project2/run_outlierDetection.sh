#!/bin/bash

echo "Removing existing output directory..."
hdfs dfs -rm -r -f /user/cs585/prj_output

echo "Running OutlierDetection job..."

# Execute from container
hadoop jar ~/shared_folder/project2/target/project2-1.0-SNAPSHOT.jar \
  dev.bfavro.job.OutlierDetection \
  /user/cs585/prj_input/datasetP.csv \
  /user/cs585/prj_output \
  10 \
  20