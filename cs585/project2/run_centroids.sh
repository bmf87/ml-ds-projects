#!/bin/bash
echo "Creating output directory..."
mkdir -p data
read -r -p "Enter the number of centroids: " pointCount
echo "Creating $pointCount centroids..."

mvn compile exec:java -Dexec.mainClass="dev.bfavro.data.Centroids" -Dexec.args="$pointCount"