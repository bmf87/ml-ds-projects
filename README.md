# CS585: Projects 

## Project 1

This Maven project generates synthetic customer and transaction datasets and analyzes them with Apache Hadoop MapReduce and Apache Pig.
Customer records contain `ID,Name,Age,Gender,CountryCode,Salary`, and transaction records contain `TransID,CustID,TransTotal,TransNumItems,TransDesc`.

Generate the datasets with `./run_datasets.sh` and build the Hadoop jar with `mvn clean package`.
The Java MapReduce jobs are in the `dev.bfavro.job` package and use the customer and transaction CSV files as inputs.

Build the jar with `mvn clean package`; Maven produces `target/project1-1.0-SNAPSHOT.jar`.
`run_*.sh` scripts are configured to run the job with the matching name.
Test suite can be run with `mvn test`.

### CustomerJoin

`CustomerJoin` accepts customer, transaction, and output paths, then emits one row per customer with transaction count, total amount, and minimum item count.

### CountrySummary

`CountrySummary` accepts the same paths and reports customer counts and minimum/maximum transaction totals by country code.

### AgeRangeSummary

`AgeRangeSummary` accepts the same paths and reports minimum, maximum, and average transaction totals by age range and gender.

### Apache Pig

Apache Pig scripts are in `pig/` and use the HDFS input paths configured at the top of each script.
`cc_custcounts.pig` reports country codes with more than 5,000 or fewer than 2,000 customers.
`cust_mintranscount.pig` reports every customer tied for the fewest transactions, while `demog_custstatistics.pig` reports transaction-total statistics by age range and gender.

## Project 2

This Maven project contains three Hadoop MapReduce jobs in the `dev.bfavro.job` package.
Point records use `x,y`, center records use `id,x,y`, and rectangle records use `id,x,y,height,width` (bottom-left coordinate, then dimensions).

Build the jar with `mvn clean package`; Maven produces `target/project2-1.0-SNAPSHOT.jar`.
`run_*.sh` scripts are configured to run the job with the matching name.
Test suite can be run with `mvn test`.


### K-Means Clustering

`dev.bfavro.job.KMeansClustering <centers> <points> <output-directory> <max-iterations>`
It assigns each point to its nearest center, updates centers to the means of their assigned points, preserves empty centers, and stops after two consecutive unchanged iterations or the iteration limit.

### Outlier Detection

`dev.bfavro.job.OutlierDetection <points> <output-directory> <radius-r> <threshold-k>`
It reports points with fewer than `k` other points within inclusive Euclidean radius `r`, using a spatial grid to limit comparisons.

### Spatial Join

`dev.bfavro.job.SpatialJoin <points> <rectangles> <output-directory> [x1,y1,x2,y2]`
It emits each rectangle ID with every contained point coordinate, and an optional window filters candidate points and rectangles.
