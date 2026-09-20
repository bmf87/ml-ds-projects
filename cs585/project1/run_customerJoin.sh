#!/bin/bash

echo "Removing existing output directory..."
hdfs dfs -rm -r -f /user/cs585/prj_output

echo "Running CustomerJoin job..."

# Execute from container
hadoop jar ~/shared_folder/project1/target/project1-1.0-SNAPSHOT.jar \
  dev.bfavro.job.CustomerJoin \
  /user/cs585/prj_input/customers.csv \
  /user/cs585/prj_input/transactions.csv \
  /user/cs585/prj_output