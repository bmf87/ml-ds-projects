# CS585: Project 1

Project 1 generates synthetic customer and transaction datasets and analyzes them with Apache Hadoop MapReduce and Apache Pig. The Java jobs perform customer-transaction joins and demographic summaries, while the Pig scripts calculate customer transaction counts, country-level customer counts, and age-range/gender transaction statistics including minimum, maximum, and average transaction totals. Input and output paths are configured for HDFS, with local tests covering the dataset generators and MapReduce aggregation logic.
