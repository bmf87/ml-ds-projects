#!/bin/bash

echo "Removing existing output directory..."
hdfs dfs -rm -r -f /user/cs585/prj_output


read -r -p "Enter max iterations for KMeans or press enter to use the default (6): " maxIterations
maxIterations=${maxIterations:-6}
echo "Running KMeansClustering job for $maxIterations iterations.."

# Execute from container
hadoop jar ~/shared_folder/project2/target/project2-1.0-SNAPSHOT.jar \
  dev.bfavro.job.KMeansClustering \
  /user/cs585/prj_input/centers.csv \
  /user/cs585/prj_input/datasetP.csv \
  /user/cs585/prj_output \
  $maxIterations