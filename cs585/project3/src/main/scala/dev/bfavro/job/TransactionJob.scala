package dev.bfavro.job

import dev.bfavro.io.TransactionIO
import dev.bfavro.model.CustomerCountStep
import dev.bfavro.output.{Result, StepWriter}
import dev.bfavro.transform.TransactionTransforms
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path

import scala.util.Try

object TransactionJob {
  val DefaultPreviewLimit: Int = 20
  val MinimumTotal: BigDecimal = BigDecimal(200)
  val HighValueMinimumTotal: BigDecimal = BigDecimal(600)
  val DefaultCountMultiplier: Long = 5L
  val DebugFileName: String = "transaction-debug-steps.txt"

  def run(
      sc: SparkContext,
      inputPath: String,
      previewLimit: Int = DefaultPreviewLimit,
      debugSteps: Boolean = false,
      multiplier: Long = DefaultCountMultiplier
  ): Result = {
    require(previewLimit > 0, "Preview limit must be a positive integer")
    require(multiplier > 0, "Count multiplier must be positive")
    val transactions = TransactionIO.load(sc, inputPath).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      // Count invalid (Left) rows
      val invalidCount = transactions.filter(_.isLeft).count()
      val validTransactions = transactions.flatMap(_.right.toOption)
      // Apply T1: filter transactions by minimum total amount ($200)
      val t1 = TransactionTransforms.filterByMinimumTotal(validTransactions, MinimumTotal)
      val t2 = TransactionTransforms.summarizeByItemCount(t1)
      val t3 = TransactionTransforms.countTransactionsByCustomer(t1)
      
      // Apply T4: filter transactions by minimum total amount ($600)
      val t4 = TransactionTransforms.filterByMinimumTotal(validTransactions, HighValueMinimumTotal)
      val t5 = TransactionTransforms.countTransactionsByCustomer(t4)
      if (debugSteps) { // cache T3 and T5 for debugging + T6 join
        t3.persist(StorageLevel.MEMORY_AND_DISK)
        t5.persist(StorageLevel.MEMORY_AND_DISK)
      }
      try {
        val t6 = TransactionTransforms.compareCustomerCounts(t3, t5, multiplier)
        Result(
          invalidCount,
          t1.take(previewLimit).toSeq,
          t2.collect().toSeq,
          t6.collect().toSeq,
          if (debugSteps) Seq(CustomerCountStep("T3", t3.collect().toSeq),
            CustomerCountStep("T5", t5.collect().toSeq)) else Seq.empty,
          if (debugSteps) Some(t4.take(previewLimit).toSeq) else None)
      } finally {
        // Unpersist cached RDDs for debugging, if necessary
        if (debugSteps) {
          t3.unpersist(blocking = true)
          t5.unpersist(blocking = true)
        }
      }
    } finally {
      // Unpersist (transactions.csv) RDD to free memory
      transactions.unpersist(blocking = true)
    }
  }

  /**
    * Prints the invalid-row count, T1 preview, T2 statistics, and enriched T6 rows,
    * saving those same blocks to stepWriter when supplied. T6 shows customer ID,
    * T5 count, T5 count times the multiplier, and T3 count, sorted by customer ID.
    *
    * Only when debugSteps is true, prints T3 counts, the bounded T4 preview, and T5
    * counts between T2 and T6, saving them exclusively to debugWriter in that order.
    * Debug data never enters the normal report. No Spark actions are performed here.
    * 
    */
  def displayResult(
      result: Result,
      stepWriter: Option[StepWriter] = None,
      debugSteps: Boolean = false,
      debugWriter: Option[StepWriter] = None
  ): Unit = {
    val blocks = Seq(
      result.formatInvalidCount(),
      result.formatPreview(),
      result.formatSummary())
    blocks.foreach { text =>
      println(text)
      stepWriter.foreach(_.saveToHdfs(text))
    }
    if (debugSteps) {
      result.customerSteps.foreach { step =>
        val text = result.formatCustomerCounts(step)
        println(text)
        debugWriter.foreach(_.saveToHdfs(text))
        if (step.name == "T3") {
          result.t4Preview.foreach { preview =>
            val debugText = result.formatPreview(preview, "T4 debug")
            println(debugText)
            debugWriter.foreach(_.saveToHdfs(debugText))
          }
        }
      }
    }
    // T6 customer comparisons
    val selectedCustomers = result.formatCustomerComparisons()
    println(selectedCustomers)
    stepWriter.foreach(_.saveToHdfs(selectedCustomers))
  }

  // Defensive default for debug output path
  def defaultDebugOutputPath(outputPath: Option[String]): String =
    outputPath.map { path =>
      val parent = new Path(path).getParent
      if (parent == null) DebugFileName else new Path(parent, DebugFileName).toString
    }.getOrElse(s"hdfs:///user/cs585/prj_output/$DebugFileName")

  def main(args: Array[String]): Unit = {
    require(args.length >= 1 && args.length <= 3,
      "Usage: TransactionJob <input-path-or-URI> [preview-limit] optional:[output-file-or-URI]")
    val previewLimit = if (args.length >= 2) {
      Try(args(1).toInt).getOrElse(
        throw new IllegalArgumentException("Preview limit must be a positive integer"))
    } else DefaultPreviewLimit
    require(previewLimit > 0, "Preview limit must be a positive integer")

    val sc = new SparkContext(new SparkConf().setAppName("Transaction Spark Job"))
    try {
      // Read configurations from command-line arguments or set defaults
      val debugSteps = sc.getConf.getBoolean("spark.transactionJob.debugSteps", false)
      val multiplier = sc.getConf.getLong("spark.transactionJob.multiplier", DefaultCountMultiplier)
      require(multiplier > 0, "Count multiplier must be positive")
      val outputPath = args.lift(2)
      val debugPath = if (debugSteps) Some(sc.getConf.get(
        "spark.transactionJob.debugOutputPath", defaultDebugOutputPath(outputPath))) else None
      for (normal <- outputPath; debug <- debugPath) {
        val normalPath = new Path(normal)
        val debugOutputPath = new Path(debug)
        require(normalPath.getFileSystem(sc.hadoopConfiguration).makeQualified(normalPath) !=
          debugOutputPath.getFileSystem(sc.hadoopConfiguration).makeQualified(debugOutputPath),
          "Normal and debug output files must be different")
      }
      val stepWriter = outputPath.map(path => new StepWriter(path, sc.hadoopConfiguration))
      try {
        val debugWriter = debugPath.map(path => new StepWriter(path, sc.hadoopConfiguration))
        try {
          val result = run(sc, args(0), previewLimit, debugSteps, multiplier)
          displayResult(result, stepWriter, debugSteps, debugWriter)
        } finally {
          debugWriter.foreach(_.close())
        }
      } finally {
        stepWriter.foreach(_.close())
      }
    } finally {
      sc.stop()
    }
  }
}