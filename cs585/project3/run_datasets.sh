#!/bin/bash
mkdir -p data

echo -e "\nCompiling and running Datasets..."
mkdir -p target/datasets-classes data && \
javac -d target/datasets-classes src/main/scala/dev/bfavro/data/*.java && \
java -cp target/datasets-classes dev.bfavro.data.Datasets
echo -e "Data generation complete.\n"