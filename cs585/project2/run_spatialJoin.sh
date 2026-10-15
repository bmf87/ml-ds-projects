#!/bin/bash

echo "Removing existing output directory..."
hdfs dfs -rm -r -f /user/cs585/prj_output

echo "Running SpatialJoin job..."

# Execute from container
hadoop jar ~/shared_folder/project2/target/project2-1.0-SNAPSHOT.jar \
  dev.bfavro.job.SpatialJoin \
  /user/cs585/prj_input/datasetP.csv \
  /user/cs585/prj_input/datasetR.csv \
  /user/cs585/prj_output