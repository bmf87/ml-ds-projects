#!/bin/bash

echo "Removing existing output directory..."
hdfs dfs -rm -r -f /user/cs585/prj_output/demog_custstatistics

# Child JVMs don't seem to inherit the PIG_CLASSPATH, explicitly pass the commons-collections jar to them
export COMMONS_COLLECTIONS=/opt/pig/lib/hadoop3-runtime/commons-collections-3.2.2.jar
export HADOOP_CLASSPATH="$COMMONS_COLLECTIONS:${HADOOP_CLASSPATH:-}"
export PIG_CLASSPATH="$COMMONS_COLLECTIONS:${PIG_CLASSPATH:-}"

# Stage Jars in MapReduce classpath
pig \
  -Dpig.additional.jars="$COMMONS_COLLECTIONS" \
  pig/demog_custstatistics.pig