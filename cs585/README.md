# CS585: Projects

| Project | Tech Stack | Domain & Focus | Key Jobs & Scripts | Directory |
| :--- | :--- | :--- | :--- | :--- |
| [Project 1](#project-1) | Java 8, Maven, Apache Hadoop MapReduce 3.4.2, Apache Pig | Relational joins & demographic aggregations over synthetic customer/transaction CSVs | `CustomerJoin`, `CountrySummary`, `AgeRangeSummary`, 3 Pig scripts | [`project1`](project1) |
| [Project 2](#project-2) | Java 8, Maven, Apache Hadoop MapReduce 3.4.2 | Iterative clustering & grid-partitioned spatial analytics in 2D space | `KMeansClustering`, `OutlierDetection`, `SpatialJoin` | [`project2`](project2) |
| [Project 3](#project-3) | Scala 2.12.18, Java 8, sbt, Apache Spark Core 3.5.6 (RDDs) | Distributed RDD pipelines for spatial grid relative density & multi-step transaction analytics | `GridCellJob`, `TransactionJob` | [`project3`](project3) |

---

## Project 1

**Directory:** [`project1`](project1)  
**Stack:** Java 8, Apache Hadoop 3.4.2 (MapReduce), Apache Pig, Maven, JUnit 4 / Mockito

Project 1 generates bounded synthetic customer and transaction datasets and analyzes them using both Java MapReduce jobs (comparing reduce-side joins vs. distributed-cache broadcast joins with combiners) and Apache Pig scripts.

### Datasets

Generated into `project1/data/` via `dev.bfavro.data.Datasets` (`./run_datasets.sh`):

- **`customers.csv`** (up to 50,000 records): `ID,Name,Age,Gender,CountryCode,Salary`
  - `Age`: `10`–`70`, `Gender`: `male` / `female`, `CountryCode`: `1`–`10`, `Salary`: `100.00`–`10,000.00`
- **`transactions.csv`** (up to 5,000,000 records): `TransID,CustID,TransTotal,TransNumItems,TransDesc`
  - `CustID` cycles evenly across customer IDs, `TransTotal`: `10.00`–`1,000.00`, `TransNumItems`: `1`–`10`, `TransDesc`: 20–50 characters

### MapReduce Jobs (`dev.bfavro.job`)

- **`CustomerJoin`** ([`run_customerJoin.sh`](project1/run_customerJoin.sh)): Performs a reduce-side join on `CustID` using `C`/`T` tagged records. Emits one row per customer with `ID, Name, Salary, TransactionCount, TransTotalSum, MinNumItems`.
- **`CountrySummary`** ([`run_countrySummary.sh`](project1/run_countrySummary.sh)): Uses a broadcast join (distributing `customers.csv` via Hadoop Distributed Cache) and a combiner to report customer counts and min/max `TransTotal` grouped by `CountryCode`.
- **`AgeRangeSummary`** ([`run_ageRangeSummary.sh`](project1/run_ageRangeSummary.sh)): Uses a broadcast join via Distributed Cache and a combiner to report min, max, and average `TransTotal` grouped by age bracket (`[10,20)`, `[20,30)`, `[30,40)`, `[40,50)`, `[50,60)`, `[60,70]`) and `Gender`.

### Apache Pig Scripts ([`project1/pig/`](project1/pig))

- **`cc_custcounts.pig`** ([`run_custcounts.sh`](project1/run_custcounts.sh)): Groups customers by `CountryCode` and reports country codes with `> 5,000` or `< 2,000` customers.
- **`cust_mintranscount.pig`** ([`run_mintranscount.sh`](project1/run_mintranscount.sh)): Joins customers and transactions, counts transactions per `(CustID, Name)`, and outputs every customer tied for the global minimum transaction count.
- **`demog_custstatistics.pig`** ([`run_custstatistics.sh`](project1/run_custstatistics.sh)): Joins customers and transactions and computes min, max, and average `TransTotal` (rounded to 2 decimal places) by age range and gender.

### Build, Test & Run (Project 1)

```bash
cd project1

# Generate synthetic datasets in data/
./run_datasets.sh

# Run unit tests and package JAR -> target/project1-1.0-SNAPSHOT.jar
mvn test
mvn clean package

# Run MapReduce jobs or Pig scripts (configured for HDFS paths under /user/cs585/)
./run_customerJoin.sh
./run_countrySummary.sh
./run_ageRangeSummary.sh
./run_custcounts.sh
./run_mintranscount.sh
./run_custstatistics.sh
```

---

## Project 2

**Directory:** [`project2`](project2)  
**Stack:** Java 8, Apache Hadoop 3.4.2 (MapReduce), Maven, JUnit 4 / Mockito

Project 2 implements iterative clustering and grid-partitioned spatial algorithms in Hadoop MapReduce over a 2D integer coordinate domain (`1..10,000`).

### Datasets

Generated into `project2/data/` via `dev.bfavro.data.Datasets` (`./run_datasets.sh`) and `dev.bfavro.data.Centroids` (`./run_centroids.sh`):

- **`datasetP.csv`** (~100 MB): 2D points formatted as `x,y`
- **`datasetR.csv`** (~100 MB): 2D rectangles formatted as `id,x,y,height,width` (bottom-left `(x,y)`, height `1..20`, width `1..7`, bounded within `1..10,000`)
- **`centers.csv`** (interactive count `K`): initial centroids formatted as `id,x,y`

### MapReduce Jobs (`dev.bfavro.job`)

- **`KMeansClustering`** ([`run_kmeansClustering.sh`](project2/run_kmeansClustering.sh))  
  Usage: `dev.bfavro.job.KMeansClustering <centers> <points> <output-directory> <max-iterations>`  
  Reads current centers from the Distributed Cache, assigns each point to its nearest centroid, uses a combiner to partially aggregate `(count, sumX, sumY)`, preserves empty centers, and halts after two consecutive unchanged iterations or `<max-iterations>` (script defaults to `6`).
- **`OutlierDetection`** ([`run_outlierDetection.sh`](project2/run_outlierDetection.sh))  
  Usage: `dev.bfavro.job.OutlierDetection <points> <output-directory> <radius-r> <threshold-k>`  
  Identifies points with fewer than `k` other points within inclusive Euclidean radius `r` (script uses `r=10`, `k=20`). Partitions space into a uniform grid, emitting each point to its home cell as a core record (`C|`) and replicating it as a support record (`S|`) to overlapping neighbor cells so each reducer evaluates every core point's full neighborhood once.
- **`SpatialJoin`** ([`run_spatialJoin.sh`](project2/run_spatialJoin.sh))  
  Usage: `dev.bfavro.job.SpatialJoin <points> <rectangles> <output-directory> [x1,y1,x2,y2]`  
  Performs a grid-partitioned point-in-rectangle join, mapping points to their containing cell and rectangles to every overlapping cell, then testing containment in the reducer. Accepts an optional `[x1,y1,x2,y2]` window to filter candidate points and rectangles.

### Build, Test & Run (Project 2)

```bash
cd project2

# Generate points/rectangles and K centroids
./run_datasets.sh
./run_centroids.sh

# Run unit tests and package JAR -> target/project2-1.0-SNAPSHOT.jar
mvn test
mvn clean package

# Run MapReduce jobs (configured for HDFS paths under /user/cs585/)
./run_kmeansClustering.sh
./run_outlierDetection.sh
./run_spatialJoin.sh
```

---

## Project 3

**Directory:** [`project3`](project3) (see also [`project3/README.md`](project3/README.md))  
**Stack:** Scala 2.12.18, Java 8, Apache Spark Core 3.5.6 (RDD APIs), sbt, ScalaTest 3.2.19

Project 3 implements distributed Spark Core RDD pipelines (without SparkSQL) for spatial grid density ranking and precision-safe multi-step transaction analytics, with sequential single-file driver output via `StepWriter`.

### Datasets

Located/generated in `project3/data/`:

- **`datasetP.csv`** (generated via `./run_datasets.sh`): headerless `x,y` integer coordinates in `1..10,000`
- **`transactions.csv`**: headerless 5-field CSV (`TransID,CustID,TransTotal,TransNumItems,TransDesc`) validated against project bounds (`TransID` `1..5,000,000`, `CustID` `1..50,000`, `TransTotal` `10..1000`, `TransNumItems` `1..10`, `TransDesc` 20–50 characters)

### Spark Core Jobs (`dev.bfavro.job`)

- **`GridCellJob`** ([`run_gridCellJob.sh`](project3/run_gridCellJob.sh))  
  Usage: `<input-path-or-URI> [top-k] [output-file-or-URI]` (default `top-k` = `50`)  
  Divides the `1..10,000` domain into a 500×500 grid of 20×20 cells (1-based row-major IDs starting at bottom-left). Uses two RDD shuffles (`reduceByKey`) to compute each cell's relative-density index `I(X) = count(X) / avg(count(N(X)))` across its 3, 5, or 8 neighbors, selects the top-`k` cells with `takeOrdered`, and enriches them with neighbor indices.
- **`TransactionJob`** ([`run_transactionJob.sh`](project3/run_transactionJob.sh))  
  Usage: `<input-path-or-URI> [preview-limit] [output-file-or-URI]` (default `preview-limit` = `20`)  
  Validates input rows with `BigDecimal` monetary arithmetic (`HALF_UP` rounding), caches parsed RDDs (`MEMORY_AND_DISK`), and executes steps **T1–T6**:
  - **T1**: Filter transactions with `TransTotal >= 200` (reports a bounded preview).
  - **T2**: Group T1 by `TransNumItems` and compute count, sum, average, min, and max totals.
  - **T3 & T5**: Count T1 (`>= 200`) and T4 (`>= 600`) transactions per `CustID` (optionally emitted to stdout and a separate debug file when `spark.transactionJob.debugSteps=true`).
  - **T6**: Distributed left outer join of T3 and T5 reporting customers where `T5.count * multiplier < T3.count` (default multiplier `5` in `TransactionJob`, `3` in `run_transactionJob.sh` via `T6_MULTIPLIER`).

### Build, Test & Run (Project 3)

```bash
cd project3

# Use Java 8 (required for Spark 3.5.6 runtime compatibility)
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

# Compile, run ScalaTest suite, and build JAR -> target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar
sbt -batch compile test package

# Run locally via sbt SparkSubmit (when spark-submit is not installed locally)
sbt -batch 'Test / runMain org.apache.spark.deploy.SparkSubmit --master local[4] --conf spark.ui.enabled=false --class dev.bfavro.job.GridCellJob target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar data/datasetP.csv 50'
sbt -batch 'Test / runMain org.apache.spark.deploy.SparkSubmit --master local[2] --conf spark.ui.enabled=false --class dev.bfavro.job.TransactionJob target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar data/transactions.csv 20'

# Or run interactively on YARN
bash run_gridCellJob.sh
bash run_transactionJob.sh
```

