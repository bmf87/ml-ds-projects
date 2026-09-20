#!/bin/bash
echo "Creating output directory..."
mkdir -p data

mvn compile exec:java -Dexec.mainClass="dev.bfavro.data.Datasets"