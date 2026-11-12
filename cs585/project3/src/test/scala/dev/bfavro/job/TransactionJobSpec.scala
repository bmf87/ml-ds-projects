package dev.bfavro.job

import dev.bfavro.io.TransactionIO
import dev.bfavro.model.{CustomerCountComparison, CustomerCountStep, ItemCountStats, Transaction}
import dev.bfavro.output.{Result, StepWriter}
import dev.bfavro.transform.TransactionTransforms
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._

class TransactionJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val description = "abcdefghijklmnopqrst"
  private var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext(new SparkConf()
      .setAppName("TransactionJobSpec")
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1"))
    sc.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (sc != null) sc.stop()
    } finally {
      super.afterAll()
    }
  }

  private def line(total: String = "200.00", items: Int = 2): String =
    s"1,2,$total,$items,$description"

  private def withInput(lines: Seq[String])(check: String => Unit): Unit = {
    val path = Files.createTempFile("transactions-", ".csv")
    try {
      Files.write(path, lines.asJava, StandardCharsets.UTF_8)
      check(path.toUri.toString)
    } finally {
      Files.deleteIfExists(path)
    }
  }

  private def withOutput(check: java.nio.file.Path => Unit): Unit = {
    val directory = Files.createTempDirectory("transaction-step-output-")
    try {
      check(directory.resolve("steps.txt"))
    } finally {
      val path = new org.apache.hadoop.fs.Path(directory.toUri)
      path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
    }
  }

  private def assertAlignedTable(
      formatted: String,
      headers: Seq[String],
      expectedRows: Seq[Seq[String]]
  ): Unit = {
    val lines = formatted.split("\n").drop(1)
    val starts = headers.map(lines.head.indexOf(_))
    assert(!formatted.contains("\t"))
    assert(lines.length == expectedRows.size + 1)
    lines.zip(headers +: expectedRows).foreach { case (row, values) =>
      headers.indices.foreach { columnIndex =>
        val end = if (columnIndex == headers.size - 1) row.length else starts(columnIndex + 1)
        assert(row.substring(starts(columnIndex), end).trim == values(columnIndex))
        assert(row.startsWith(values(columnIndex), starts(columnIndex)))
      }
    }
  }

  test("parser reads the headerless five-column schema without losing decimal precision") {
    assert(TransactionIO.parseLine(line()) ==
      Right(Transaction(1, 2, BigDecimal("200.00"), 2, description)))
    Seq("199.99", "200.00", "200.01").foreach { amount =>
      assert(TransactionIO.parseLine(line(amount)).right.get.transTotal == BigDecimal(amount))
    }
  }

  test("parser rejects malformed rows and unsupported quoting") {
    Seq("", "1,2,200,2", line() + ",extra", "1,2,200,2,",
      line("NaN"), line("not-a-number"), s"""1,2,200,2,"$description""""",
      s"TransID,CustID,TransTotal,TransNumItems,TransDesc",
      s"bad,2,200,2,$description").foreach { invalid =>
      assert(TransactionIO.parseLine(invalid).isLeft, invalid)
    }
  }

  test("parser enforces documented ranges and description length boundaries") {
    Seq(line("9.99"), line("1000.01"), line(items = 0), line(items = 11),
      s"0,2,200,2,$description", s"5000001,2,200,2,$description",
      s"1,0,200,2,$description", s"1,50001,200,2,$description",
      "1,2,200,2," + "a" * 19, "1,2,200,2," + "a" * 51).foreach { invalid =>
      assert(TransactionIO.parseLine(invalid).isLeft, invalid)
    }
    assert(TransactionIO.parseLine("5000000,50000,1000,10," + "a" * 50).isRight)
    assert(TransactionIO.parseLine(line("10", 1)).isRight)
  }

  test("T1 includes exactly 200 and excludes amounts below the threshold") {
    val transactions = Seq("199.99", "200.00", "200.01")
      .map(amount => Transaction(1, 2, BigDecimal(amount), 2, description))
    val result = TransactionTransforms.filterByMinimumTotal(
      sc.parallelize(transactions, 2), BigDecimal(200)).collect()
    assert(result.map(_.transTotal).toSet == Set(BigDecimal("200.00"), BigDecimal("200.01")))
  }

  test("T2 summarizes T1 correctly across partition counts") {
    val transactions = Seq(
      Transaction(1, 2, BigDecimal("199.99"), 2, description),
      Transaction(2, 2, BigDecimal("200.00"), 2, description),
      Transaction(3, 2, BigDecimal("300.00"), 2, description),
      Transaction(4, 2, BigDecimal("250.00"), 3, description))
    Seq(1, 2, 4).foreach { partitions =>
      val t1 = TransactionTransforms.filterByMinimumTotal(
        sc.parallelize(transactions, partitions), BigDecimal(200))
      val result = TransactionTransforms.summarizeByItemCount(t1).collect().sortBy(_.itemCount)
      assert(result.toSeq == Seq(
        ItemCountStats(2, 2L, BigDecimal(500), BigDecimal(200), BigDecimal(300)),
        ItemCountStats(3, 1L, BigDecimal(250), BigDecimal(250), BigDecimal(250))))
    }
  }

  test("empty input and no surviving T1 produce no groups") {
    val inputs = Seq(Seq.empty[Transaction],
      Seq(Transaction(1, 2, BigDecimal("199.99"), 2, description)))
    inputs.foreach { transactions =>
      val t1 = TransactionTransforms.filterByMinimumTotal(
        sc.parallelize(transactions, 2), BigDecimal(200))
      assert(TransactionTransforms.summarizeByItemCount(t1).collect().isEmpty)
    }
  }

  test("T2 preserves exact decimal sums without averaging partial averages") {
    val transactions = Seq("200.01", "200.02", "300.03", "400.04", "500.05")
      .map(amount => Transaction(1, 2, BigDecimal(amount), 2, description))
    val result = TransactionTransforms.summarizeByItemCount(
      sc.parallelize(transactions, 3)).collect().head
    assert(result.transactionCount == 5L)
    assert(result.totalSum == BigDecimal("1600.15"))
    assert(result.minimum == BigDecimal("200.01"))
    assert(result.maximum == BigDecimal("500.05"))
  }

  test("countTransactionsByCustomer counts T1 by customer across partition counts") {
    val transactions = Seq(
      Transaction(1, 7, BigDecimal("199.99"), 2, description),
      Transaction(2, 7, BigDecimal("200.00"), 3, description),
      Transaction(3, 9, BigDecimal("300.00"), 3, description),
      Transaction(4, 7, BigDecimal("400.00"), 5, description),
      Transaction(5, 11, BigDecimal("100.00"), 2, description))
    Seq(1, 2, 4).foreach { partitions =>
      val t1 = TransactionTransforms.filterByMinimumTotal(
        sc.parallelize(transactions, partitions), BigDecimal(200))
      val result = TransactionTransforms.countTransactionsByCustomer(t1).collect().toMap
      assert(result == Map(7 -> 2L, 9 -> 1L))
    }
  }

  test("countTransactionsByCustomer handles empty and fully filtered input") {
    val inputs = Seq(Seq.empty[Transaction],
      Seq(Transaction(1, 7, BigDecimal("199.99"), 2, description)))
    inputs.foreach { transactions =>
      val t1 = TransactionTransforms.filterByMinimumTotal(
        sc.parallelize(transactions, 2), BigDecimal(200))
      assert(TransactionTransforms.countTransactionsByCustomer(t1).collect().isEmpty)
    }
  }

  test("job counts invalid rows reliably, bounds preview, and releases cached input") {
    withInput(Seq(line("199.99"), line("200.00"), line("300.00"),
      line("250.00", 3), "bad", "1,2,200,2,")) { path =>
      val cachedBefore = sc.getPersistentRDDs.keySet
      Seq(1, 2).foreach { _ =>
        val result = TransactionJob.run(sc, path, previewLimit = 1)
        assert(result.invalidRowCount == 2L)
        assert(result.preview.size == 1)
        assert(result.preview.forall(_.transTotal >= 200))
        assert(result.summaries.sortBy(_.itemCount) == Seq(
          ItemCountStats(2, 2L, BigDecimal(500), BigDecimal(200), BigDecimal(300)),
          ItemCountStats(3, 1L, BigDecimal(250), BigDecimal(250), BigDecimal(250))))
        assert(sc.getPersistentRDDs.keySet == cachedBefore)
      }
      assert(!sc.isStopped)
    }
  }

  test("job handles empty, all-invalid, and fully filtered inputs") {
    Seq(Seq.empty[String], Seq("bad", ""), Seq(line("199.99"))).foreach { lines =>
      withInput(lines) { path =>
        val result = TransactionJob.run(sc, path)
        assert(result.invalidRowCount == lines.count(TransactionIO.parseLine(_).isLeft).toLong)
        assert(result.preview.isEmpty)
        assert(result.summaries.isEmpty)
        assert(result.customerSteps.isEmpty)
        assert(result.selectedCustomers.isEmpty)
        assert(result.t4Preview.isEmpty)
      }
    }
  }

  test("input failures propagate and cached input is released on failure") {
    withInput(Seq.empty) { path =>
      val cachedBefore = sc.getPersistentRDDs.keySet
      intercept[org.apache.hadoop.mapred.InvalidInputException] {
        TransactionJob.run(sc, path + ".missing")
      }
      assert(sc.getPersistentRDDs.keySet == cachedBefore)
    }
  }

  test("invalid CLI arguments and preview limits fail before starting work") {
    Seq(Array.empty[String], Array("input", "bad"), Array("input", "0"),
      Array("input", "-1"), Array("input", "20", "output", "extra")).foreach { args =>
      intercept[IllegalArgumentException](TransactionJob.main(args))
    }
    intercept[IllegalArgumentException](TransactionJob.run(sc, "unused", 0))
  }

  test("display orders groups and rounds averages only for presentation") {
    val summaries = Seq(
      ItemCountStats(3, 3L, BigDecimal("600.02"), BigDecimal(200), BigDecimal("200.02")),
      ItemCountStats(2, 2L, BigDecimal("400.01"), BigDecimal(200), BigDecimal("200.01")))
    val result = Result(2L, Seq(Transaction(1, 2, BigDecimal(200), 2, description)), summaries)
    val formatted = result.formatSummary().split("\n")
    assert(formatted(2).split(" +").toSeq == Seq("2", "2", "400.01", "200.01", "200.00", "200.01"))
    assert(formatted(3).split(" +").toSeq == Seq("3", "3", "600.02", "200.01", "200.00", "200.02"))
    assert(summaries.head.totalSum == BigDecimal("600.02"))
    assert(result.formatSummary(Seq.empty).contains("no matching groups"))
    assert(result.formatPreview(Seq.empty).contains("no matching transactions"))
    assert(result.formatInvalidCount() == "Skipped invalid rows: 2")
    assert(result.formatPreview() == result.formatPreview(result.preview))
    assert(result.formatSummary() == result.formatSummary(result.summaries))
    val preview = result.formatPreview()
    assert(preview.split("\n").last.split(" +").toSeq == Seq("1", "2", "200.00", "2", description))
  }

  test("preview values align with column headers regardless of ID and amount length") {
    val result = Result(0L, Seq(
      Transaction(1, 1, BigDecimal("950.26"), 8, description),
      Transaction(5000000, 50000, BigDecimal("1000.00"), 10, description)), Seq.empty)
    val formatted = result.formatPreview()
    val lines = formatted.split("\n").drop(1)
    val headers = Seq("TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc")
    val starts = headers.map(lines.head.indexOf(_))
    val expected = Seq(headers,
      Seq("1", "1", "950.26", "8", description),
      Seq("5000000", "50000", "1000.00", "10", description))
    assert(!formatted.contains("\t"))
    lines.zip(expected).foreach { case (row, values) =>
      headers.indices.foreach { columnIndex =>
        val end = if (columnIndex == headers.size - 1) row.length else starts(columnIndex + 1)
        assert(row.substring(starts(columnIndex), end).trim == values(columnIndex))
      }
    }
    assert(result.formatPreview(result.preview, "T4 debug").split("\n").drop(1).toSeq == lines.toSeq)
  }

  test("StepWriter preserves five formatted steps in order in one UTF-8 file") {
    withOutput { path =>
      val result = Result(0L,
        Seq(Transaction(1, 2, BigDecimal(200), 2, description)),
        Seq(ItemCountStats(2, 1L, BigDecimal(200), BigDecimal(200), BigDecimal(200))))
      val steps = Seq(
        result.formatPreview(),
        result.formatSummary(),
        "T3:\nCaf\u00e9", "T4:\nFourth step\n", "T5:\nFifth step")
      val writer = new StepWriter(path.toUri.toString, sc.hadoopConfiguration)
      try {
        steps.foreach(writer.saveToHdfs)
      } finally {
        writer.close()
      }
      val expected = steps.map(block => if (block.endsWith("\n")) block else block + "\n").mkString
      assert(new String(Files.readAllBytes(path), StandardCharsets.UTF_8) == expected)
    }
  }

  test("StepWriter refuses to overwrite a previous run's output") {
    withOutput { path =>
      Files.write(path, "Previous results".getBytes(StandardCharsets.UTF_8))
      intercept[org.apache.hadoop.fs.FileAlreadyExistsException] {
        new StepWriter(path.toUri.toString, sc.hadoopConfiguration)
      }
      assert(new String(Files.readAllBytes(path), StandardCharsets.UTF_8) == "Previous results")
    }
  }

  test("StepWriter closes safely and rejects writes after close") {
    withOutput { path =>
      val writer = new StepWriter(path.toUri.toString, sc.hadoopConfiguration)
      writer.saveToHdfs("T1: completed")
      writer.close()
      writer.close()
      intercept[java.io.IOException](writer.saveToHdfs("T2: too late"))
      assert(new String(Files.readAllBytes(path), StandardCharsets.UTF_8) == "T1: completed\n")
    }
  }

  test("driver optionally saves the same formatted results it prints") {
    val result = Result(2L,
      Seq(Transaction(1, 2, BigDecimal(200), 2, description)),
      Seq(ItemCountStats(2, 1L, BigDecimal(200), BigDecimal(200), BigDecimal(200))))
    val expected = Seq(result.formatInvalidCount(), result.formatPreview(),
      result.formatSummary(), result.formatCustomerComparisons()).mkString("", "\n", "\n")
    Seq(false, true).foreach { save =>
      withOutput { path =>
        val writer = if (save) Some(new StepWriter(path.toUri.toString, sc.hadoopConfiguration))
                     else None
        val console = new java.io.ByteArrayOutputStream()
        try {
          Console.withOut(console) {
            TransactionJob.displayResult(result, writer)
          }
        } finally {
          writer.foreach(_.close())
        }
        assert(console.toString("UTF-8") == expected)
        if (save) {
          assert(new String(Files.readAllBytes(path), StandardCharsets.UTF_8) == expected)
        } else {
          assert(!Files.exists(path))
        }
      }
    }
  }

  test("T3 through T6 use correct thresholds, customer counts, and strict comparison") {
    val amounts = Seq.fill(5)((1, "599.99")) ++ Seq((1, "600.00")) ++
      Seq.fill(4)((2, "200.00")) ++ Seq((2, "600.01"), (3, "300.00"),
        (4, "199.99"), (5, "700.00"), (5, "1000.00"))
    val lines = amounts.zipWithIndex.map { case ((customerId, amount), index) =>
      s"${index + 1},$customerId,$amount,2,$description"
    } :+ "bad"
    withInput(lines) { path =>
      val cachedBefore = sc.getPersistentRDDs.keySet
      val result = TransactionJob.run(sc, path, previewLimit = 2,
        debugSteps = true, multiplier = 5L)
      assert(result.invalidRowCount == 1L)
      assert(result.customerSteps.map(_.name) == Seq("T3", "T5"))
      assert(result.customerSteps(0).counts.toMap == Map(1 -> 6L, 2 -> 5L, 3 -> 1L, 5 -> 2L))
      assert(result.customerSteps(1).counts.toMap == Map(1 -> 1L, 2 -> 1L, 5 -> 2L))
      assert(result.selectedCustomers.sortBy(_.customerId) ==
        Seq(CustomerCountComparison(1, 6L, 1L, 5L), CustomerCountComparison(3, 1L, 0L, 5L)))
      assert(result.t4Preview.get.size == 2)
      assert(result.t4Preview.get.map(_.transTotal) == Seq(BigDecimal("600.00"), BigDecimal("600.01")))
      assert(sc.getPersistentRDDs.keySet == cachedBefore)
      val normal = TransactionJob.run(sc, path, multiplier = 5L)
      assert(normal == result.copy(preview = normal.preview, customerSteps = Seq.empty, t4Preview = None))
      assert(normal.customerSteps.isEmpty)
      assert(normal.t4Preview.isEmpty)
      assert(sc.getPersistentRDDs.keySet == cachedBefore)
    }
  }

  test("T6 comparison joins partitions, includes zero T5 counts, and excludes equality") {
    Seq(1, 2, 4).foreach { partitions =>
      val baseline = sc.parallelize(Seq(1 -> 6L, 2 -> 5L, 3 -> 1L, 4 -> 2L), partitions)
      val comparison = sc.parallelize(Seq(1 -> 1L, 2 -> 1L, 4 -> 2L, 5 -> 1L), partitions)
      val result = TransactionTransforms.selectCustomersByCountComparison(
        baseline, comparison, 5L).collect().sorted
      assert(result.toSeq == Seq(1, 3))
    }
    assert(TransactionTransforms.selectCustomersByCountComparison(
      sc.emptyRDD[(Int, Long)], sc.emptyRDD[(Int, Long)], 5L).collect().isEmpty)
  }

  test("compareCustomerCounts retains counts and multiplier for qualifying T6 rows") {
    Seq(1, 2, 4).foreach { partitions =>
      val baseline = sc.parallelize(Seq(1 -> 6L, 2 -> 5L, 3 -> 1L), partitions)
      val comparison = sc.parallelize(Seq(1 -> 1L, 2 -> 1L, 4 -> 1L), partitions)
      val result = TransactionTransforms.compareCustomerCounts(baseline, comparison, 5L)
        .collect().sortBy(_.customerId)
      assert(result.toSeq == Seq(CustomerCountComparison(1, 6L, 1L, 5L),
        CustomerCountComparison(3, 1L, 0L, 5L)))
      assert(result.map(_.weightedComparisonCount).toSeq == Seq(BigInt(5), BigInt(0)))
    }
    val baseline = sc.parallelize(Seq(1 -> Long.MaxValue))
    val comparison = sc.parallelize(Seq(1 -> Long.MaxValue))
    assert(TransactionTransforms.compareCustomerCounts(baseline, comparison, 5L).collect().isEmpty)
    intercept[IllegalArgumentException] {
      TransactionTransforms.compareCustomerCounts(baseline, comparison, 0L)
    }
  }

  test("debugSteps gates T3 T4 T5 and saves them exclusively to a separate file") {
    val result = Result(0L, Seq.empty, Seq.empty,
      Seq(CustomerCountComparison(3, 1L, 0L, 3L), CustomerCountComparison(1, 6L, 1L, 3L)),
      Seq(CustomerCountStep("T3", Seq(2 -> 5L, 1 -> 6L)),
        CustomerCountStep("T5", Seq(2 -> 1L, 1 -> 1L))),
      Some(Seq(Transaction(1, 1, BigDecimal(600), 2, description))))
    Seq(false, true).foreach { debugSteps => withOutput { path =>
      val debugPath = path.resolveSibling(TransactionJob.DebugFileName)
      val writer = new StepWriter(path.toUri.toString, sc.hadoopConfiguration)
      val debugWriter = if (debugSteps) Some(new StepWriter(debugPath.toUri.toString,
        sc.hadoopConfiguration)) else None
      val console = new java.io.ByteArrayOutputStream()
      try {
        Console.withOut(console) {
          TransactionJob.displayResult(result, Some(writer), debugSteps, debugWriter)
        }
      } finally {
        try debugWriter.foreach(_.close()) finally writer.close()
      }
      val saved = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      val printed = console.toString("UTF-8")
      val reports = Seq(result.formatInvalidCount(), result.formatPreview(),
        result.formatSummary(), result.formatCustomerComparisons())
      assert(saved == reports.mkString("", "\n", "\n"))
      Seq("T3:", "T4 debug", "T5:").foreach(label => assert(!saved.contains(label)))
      assert(saved.endsWith(result.formatCustomerComparisons() + "\n"))
      if (debugSteps) {
        val debugSaved = new String(Files.readAllBytes(debugPath), StandardCharsets.UTF_8)
        val expectedDebug = Seq(result.formatCustomerCounts(result.customerSteps(0)),
          result.formatPreview(result.t4Preview.get, "T4 debug"),
          result.formatCustomerCounts(result.customerSteps(1))).mkString("", "\n", "\n")
        assert(debugSaved == expectedDebug)
        Seq("T1", "T2:", "T6:", "Skipped invalid").foreach(label => assert(!debugSaved.contains(label)))
        assert(printed.indexOf("T3:") < printed.indexOf("T4 debug"))
        assert(printed.indexOf("T4 debug") < printed.indexOf("T5:"))
        assert(printed.indexOf("T5:") < printed.indexOf("T6:"))
      } else {
        assert(printed == saved)
        assert(!Files.exists(debugPath))
      }
    } }
  }

  test("debug output defaults to a separate sibling file") {
    assert(TransactionJob.defaultDebugOutputPath(Some("hdfs://cluster/reports/results.txt")) ==
      "hdfs://cluster/reports/transaction-debug-steps.txt")
    assert(TransactionJob.defaultDebugOutputPath(Some("results.txt")) == TransactionJob.DebugFileName)
    assert(TransactionJob.defaultDebugOutputPath(None) ==
      "hdfs:///user/cs585/prj_output/transaction-debug-steps.txt")
  }

  test("run uses the current default multiplier and omits debug collections") {
    val amounts = Seq.fill(3)((7, "300.00")) ++ Seq((7, "600.00")) ++
      Seq.fill(2)((8, "300.00")) ++ Seq((8, "600.00"))
    val lines = amounts.zipWithIndex.map { case ((customerId, amount), index) =>
      s"${index + 1},$customerId,$amount,2,$description"
    }
    withInput(lines) { path =>
      val result = TransactionJob.run(sc, path)
      val expected = if (TransactionJob.DefaultCountMultiplier < 4L)
        Seq(CustomerCountComparison(7, 4L, 1L, TransactionJob.DefaultCountMultiplier)) else Seq.empty
      assert(result.selectedCustomers == expected)
      assert(result.customerSteps.isEmpty)
      assert(result.t4Preview.isEmpty)
      intercept[IllegalArgumentException] {
        TransactionJob.run(sc, path, multiplier = 0L)
      }
    }
    val result = Result(0L, Seq.empty, Seq.empty)
    assertAlignedTable(result.formatCustomerComparisons(
      Seq(CustomerCountComparison(1, 6L, 1L, 5L))),
      Seq("CustID", "T5.count", "T5.count * 5", "T3.count"), Seq(Seq("1", "1", "5", "6")))
    assert(result.formatCustomerComparisons() == "T6: no matching customers.")
  }

  test("T2 columns align for large sums and retain sorted groups and monetary precision") {
    val result = Result(0L, Seq.empty, Seq(
      ItemCountStats(10, 1L, BigDecimal(1000), BigDecimal(1000), BigDecimal(1000)),
      ItemCountStats(1, 404741L, BigDecimal("242913025.05"), BigDecimal(200), BigDecimal("999.99"))))
    assertAlignedTable(result.formatSummary(),
      Seq("Items", "Transactions", "Sum", "Average", "Min", "Max"),
      Seq(Seq("1", "404741", "242913025.05", "600.17", "200.00", "999.99"),
        Seq("10", "1", "1000.00", "1000.00", "1000.00", "1000.00")))
  }

  test("T6 columns align for products wider than headers and retain sorted customers") {
    val result = Result(0L, Seq.empty, Seq.empty, Seq(
      CustomerCountComparison(50000, Long.MaxValue, 3000000000000000000L, 3L),
      CustomerCountComparison(98, 80L, 26L, 3L)))
    assertAlignedTable(result.formatCustomerComparisons(),
      Seq("CustID", "T5.count", "T5.count * 3", "T3.count"),
      Seq(Seq("98", "26", "78", "80"),
        Seq("50000", "3000000000000000000", "9000000000000000000", Long.MaxValue.toString)))
  }
}