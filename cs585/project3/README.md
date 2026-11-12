# Grid Cell Relative-Density Job

`dev.bfavro.job.GridCellJob` reports the top-k (default 50) grid cells of
`data/datasetP.csv` (headerless `x,y` integers in 1..10,000) by relative-density
index `I(X) = X.count / avg(count of N(X))`, as `(CellID, RelativeDensityIndex)` tuples.

### Cell ID convention

The space is a 500x500 grid of 20x20 cells. IDs are **1-based and row-major,
starting at the bottom-left cell** (the cell touching the origin is ID 1, and IDs
increase along X first, then Y):

```
col = (x - 1) / 20          row = (y - 1) / 20          (integer division, 0-based)
id  = row * 500 + col + 1
row = (id - 1) / 500        col = (id - 1) % 500
```

So cell 1 covers x,y in 1..20, cell 2 covers x 21..40 / y 1..20, and cell 501 covers
x 1..20 / y 21..40. Neighbors are all cells at row ±1 and column ±1 (excluding the cell
itself) that stay inside 0..499, e.g. `N(1) = {2, 501, 502}` and
`N(1002) = {501, 502, 503, 1001, 1003, 1501, 1502, 1503}`.

### Distributed workflow (Spark Core RDDs)

1. Parse each line to a cell ID in parallel; malformed/out-of-range rows are counted and skipped.
2. `reduceByKey` counts points per cell.
3. Each non-empty cell emits `(self, (count, 0))` and `(neighbor, (0, count))` for each
   of its neighbors; a second `reduceByKey` yields `(ownCount, neighborSum)` per cell.
4. `I = ownCount / (neighborSum / |N(X)|)`, where `|N(X)|` is 3, 5, or 8 by formula,
   so empty neighbors correctly count as zero.
5. `takeOrdered(k)` selects the top cells (index descending, cell ID ascending on ties)
   without collecting all cells to the driver.

Cells that are empty and surrounded only by empty cells (0/0) are omitted; a non-empty
cell whose neighbors are all empty gets index `Infinity`.

### Run

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && \
sbt -batch 'testOnly dev.bfavro.job.GridCellJobSpec' package && \
sbt -batch 'Test / runMain org.apache.spark.deploy.SparkSubmit --master local[4] --conf spark.ui.enabled=false --class dev.bfavro.job.GridCellJob target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar data/datasetP.csv 50'
```

Arguments: `<input-path-or-URI> [top-k] [output-file-or-URI]`. On YARN, `bash run_gridCellJob.sh`
reads `/user/cs585/prj_input/datasetP.csv` and optionally writes
`hdfs:///user/cs585/prj_output/grid-cell-top.txt`.

# Transaction Job

Scala 2.12.18 / Spark 3.5.6 job using Spark Core RDDs, not SparkSQL.
The existing DataFrame example and its runner are unchanged.

## Input Contract

The input is headerless, one transaction per line, with five comma-separated fields:

| Field | Type | Accepted values |
| --- | --- | --- |
| TransID | Int | 1 through 5,000,000 |
| CustID | Int | 1 through 50,000 |
| TransTotal | Decimal | 10 through 1000, inclusive |
| TransNumItems | Int | 1 through 10 |
| TransDesc | String | 20 through 50 characters |

The supplied simple format does not support quoted fields, embedded commas, or
multiline records. Blank lines, headers, malformed numbers, and out-of-range
values are skipped and counted. Description text is preserved. Global ID
uniqueness and sequential completeness are not checked. File access failures
fail the job instead of being treated as invalid records.

## Processing and Output

- T1 retains transactions with `TransTotal >= 200`, including exactly 200.
- T2 groups T1 by `TransNumItems`, calculating sum, average, minimum, and maximum
  totals. A transaction count is also displayed for each group.
- T3 counts T1 transactions by customer ID.
- T4 independently filters valid input transactions, retaining totals >= 600,
  including exactly 600. Only a bounded preview is included in optional diagnostics.
- T5 counts T4 transactions by customer ID.
- T6 reports customers for which `T5.count * multiplier < T3.count`. Equality is excluded.
  A customer present in T3 but absent from T5 has T5 count zero and qualifies.
- The driver's multiplier defaults to 5 and can be
  set with `--conf spark.transactionJob.multiplier=5` or `T6_MULTIPLIER=5` in the runner.
- T3 and T5 debug reports contain `CustID` and `Transactions`. T6 shows `CustID`,
  `T5.count`, `T5.count * multiplier`, and `T3.count`; the header includes the actual
  multiplier. The product uses BigInt to prevent integer overflow.
  All are displayed in ascending customer-ID order. Each customer aggregation is
  bounded to 50,000 rows by the validated schema; raw transactions are not collected.
- Amounts are parsed as `BigDecimal`, avoiding binary floating-point aggregation
  errors. Averages are calculated from each group's sum and count, not from
  partial averages. Monetary display uses two decimal places and `HALF_UP` rounding.
- Only a bounded T1 preview reaches the driver; its default limit is 20. The limit
  does not restrict T2: T2 uses all of T1. Preview order is not guaranteed.
- T2 displays all present groups sorted by item count. Validated item counts bound
  this collection to ten groups. Absent groups are not synthesized.

## Build and Test

Use a Spark 3.5.6-supported JDK. These commands have been tested with Java 8
(8u504); Java 25/27 are not supported runtimes for this Spark version.
On this Mac, select the installed Java 8 explicitly:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) && \
sbt -batch compile 'testOnly dev.bfavro.job.TransactionJobSpec' package
```

Spark dependencies remain `Provided`, so the packaged JAR requires a matching
Spark installation when submitted. Tests use a forked JVM and one `local[2]`
SparkContext, with fixtures for schema validation, the 200 boundary, partition
merges, decimal arithmetic, skipped records, empty results, formatting, and cleanup.

## Run Locally

From the project directory, with Spark 3.5.6 available:

```bash
spark-submit --master 'local[2]' \
  --class dev.bfavro.job.TransactionJob \
  target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar \
  data/transactions.csv 20
```

If `spark-submit` is not installed locally, sbt can invoke Spark's submit entry
point using its already-resolved test classpath (which includes `Provided`
dependencies). Package the application first, then run:

```bash
sbt -batch 'Test / runMain org.apache.spark.deploy.SparkSubmit --master local[2] --conf spark.ui.enabled=false --class dev.bfavro.job.TransactionJob target/scala-2.12/project3_2.12-0.1.0-SNAPSHOT.jar data/transactions.csv 20'
```

The application accepts `<input-path-or-URI> [preview-limit] [output-file-or-URI]`; the preview limit
must be a positive integer. The Spark master is supplied by the launcher, never
hardcoded in the application.

## Run on YARN

Build the JAR and start the required Hadoop/YARN daemons using the course
container's startup script. The runner reads `/user/cs585/prj_input/transactions.csv`
and prompts for a preview limit and whether to save the normal report:

```bash
bash run_transactionJob.sh
```

The runner also prompts whether to write results to HDFS. Answer `y` to use
`hdfs:///user/cs585/prj_output/transaction-steps.txt`; the default answer `n`
does not create a writer when no output argument is supplied.

To enable T3, T4, and T5 diagnostics and create a separate debug file:

```bash
DEBUG_STEPS=true bash run_transactionJob.sh
```

For direct spark-submit usage, supply `--conf spark.transactionJob.debugSteps=true`.
The old debugT4 property is no longer used. Diagnostics are disabled by default.
When enabled, T3 counts, the bounded T4 preview, and T5 counts are printed to stdout
and saved only in the debug file, in that order. T4 uses the same preview limit as T1.

With a normal output path, the default debug file is `transaction-debug-steps.txt`
in the same directory. With no normal output path it defaults to
`hdfs:///user/cs585/prj_output/transaction-debug-steps.txt`, so debug mode creates
a file even when the normal HDFS prompt is answered `n`.
Override it using `--conf spark.transactionJob.debugOutputPath=<file-URI>` or:

```bash
DEBUG_STEPS=true \
DEBUG_OUTPUT_PATH=hdfs:///user/cs585/prj_output/debug-run-2.txt \
bash run_transactionJob.sh
```

Debug files are not automatically deleted or overwritten. Choose a new path for
each run, or explicitly remove a previous debug report you no longer need.
Normal and debug output paths must identify different files.

The runner uses YARN client mode. A Mac-local input file is not automatically
available to cluster executors. The output path above is an example; choose a
location you can write to. HDFS output requires verification on your cluster.

## Sequential Step Output

`dev.bfavro.output.StepWriter` writes driver-side formatted text to one UTF-8 file,
not a directory of Spark part files. Reuse one writer for all reported steps; each
`saveToHdfs` call preserves earlier text and writes the next block in call order.
There is no fixed limit on the number of steps. For example, inside the driver:

```scala
val stepWriter = new StepWriter(outputPath, sc.hadoopConfiguration)
try {
  stepWriter.saveToHdfs(result.formatPreview())
  stepWriter.saveToHdfs(result.formatSummary())
} finally {
  stepWriter.close()
}
```

Add subsequent steps' formatted strings before the same `finally` block, and
omit a save call for any result you do not want written. This writer does not
perform Spark actions or collect RDDs; its callers provide already-formatted text.
Saving the T1 preview still writes only the preview, not every transaction in T1.

The driver's normal report contains only the invalid-row count, T1 preview,
T2 summaries, and enriched T6 comparisons. T3/T4/T5 never enter that file.
When debugSteps is true, stdout also includes the diagnostics between T2 and T6.
`Result` owns the data and pure string formatting. `TransactionJob.displayResult`
prints those strings and optionally saves them; `main` owns
both writers and closes them before stopping Spark, including on failure.

Each save ensures a trailing newline and requests flush/hflush. HDFS readers can
see flushed content, although other filesystem wrappers may buffer until close.
The writer holds one stream open rather than reopening or appending the file.
Existing files are never overwritten or appended to: use a new filename per run,
or explicitly manage previous results yourself. If a job fails, its output file
may contain only the steps completed so far; there is no rollback.

## Extension Points

| Component | Responsibility |
| --- | --- |
| `dev.bfavro.model.Transaction` | Typed transaction data |
| `dev.bfavro.model.ItemCountStats` | Unrounded grouped totals and counts |
| `dev.bfavro.model.CustomerCountStep` | Named customer-count reports for T3 and T5 |
| `dev.bfavro.model.CustomerCountComparison` | T6 customer counts, multiplier, and calculated product |
| `dev.bfavro.io.TransactionIO` | Pure parsing and reusable RDD loading |
| `dev.bfavro.transform.TransactionTransforms` | Lazy, reusable filtering and aggregation |
| `dev.bfavro.output.Result` | Completed results, optional diagnostics, and pure string formatting |
| `dev.bfavro.output.StepWriter` | Sequential driver-owned file output |
| `dev.bfavro.job.TransactionJob` | Execution lifecycle, caching, actions, and CLI |

`TransactionJob.run` returns one `dev.bfavro.output.Result` containing the invalid
count, T1 preview, T2 statistics, T6 comparisons, optional customer steps, and a
T4 preview. There is no separate DebugResults wrapper or TransactionDisplay object.
With debugSteps false, the diagnostic fields are empty and
the T3/T5 collection and T4 preview actions are skipped entirely.
Call `result.formatInvalidCount()`, `result.formatPreview()`,
`result.formatSummary()`, or `result.formatCustomerComparisons()` to format the
stored values. Explicit arguments remain supported, such as
`result.formatPreview(result.preview)` and
`result.formatPreview(result.t4Preview.get, "T4 debug")` when a debug preview is present.
Use `result.formatCustomerCounts(step)` for a named T3 or T5 report.
These methods return strings without printing, writing files, mutating results,
or triggering Spark actions; normal/debug file separation remains driver-controlled.
`run` does not print and leaves its caller's SparkContext open.
`main` formats and prints those results and owns the
context's shutdown. Parsed records are cached with `MEMORY_AND_DISK` across the
invalid-row count, preview, and aggregation actions, then unpersisted in `finally`.
In debug mode, T3 and T5 aggregates are also cached because each is collected for
diagnostics and reused in T6's distributed left outer join; both are unpersisted
in `finally`. Outside debug mode, each aggregate is used only by T6, so no additional
aggregate cache is created.
T1 is not separately cached because its filter is inexpensive. Subsequent tasks
can compose the transformation helpers before triggering their own actions.