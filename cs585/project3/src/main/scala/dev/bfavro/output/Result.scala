package dev.bfavro.output

import dev.bfavro.model.{CustomerCountComparison, CustomerCountStep, ItemCountStats, Transaction}
import dev.bfavro.output.TextTable.formatTable

/**
  * Completed transaction-job data and its text formatting, including optional T3-T5 diagnostics.
  * Formatters return strings without printing, writing files, or performing Spark actions.
  * By default, they format this instance's data; explicit arguments can format another value.
  * The driver controls whether diagnostic data is collected, displayed, or saved separately.
  */
final case class Result(
    invalidRowCount: Long,
    preview: Seq[Transaction],
    summaries: Seq[ItemCountStats],
    selectedCustomers: Seq[CustomerCountComparison] = Seq.empty,
    customerSteps: Seq[CustomerCountStep] = Seq.empty,
    t4Preview: Option[Seq[Transaction]] = None
) {
  def formatInvalidCount(count: Long = invalidRowCount): String = s"Skipped invalid rows: $count"

  def formatPreview(
      transactions: Seq[Transaction] = preview,
      stepName: String = "T1"
  ): String = {
    if (transactions.isEmpty) {
      s"$stepName preview: no matching transactions."
    } else {
      val headers = Seq("TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc")
      val rows = transactions.map { transaction =>
        Seq(transaction.transId.toString, transaction.custId.toString,
          money(transaction.transTotal), transaction.transNumItems.toString,
          transaction.transDesc)
      }
      s"$stepName preview (${transactions.size} rows):\n${formatTable(headers, rows)}"
    }
  }

  def formatSummary(summaries: Seq[ItemCountStats] = this.summaries): String = {
    if (summaries.isEmpty) {
      "T2: no matching groups."
    } else {
      val rows = summaries.sortBy(_.itemCount).map { summary =>
        val average = summary.totalSum.bigDecimal.divide(
          java.math.BigDecimal.valueOf(summary.transactionCount),
          2, java.math.RoundingMode.HALF_UP).toPlainString
        Seq(summary.itemCount.toString, summary.transactionCount.toString,
          money(summary.totalSum), average, money(summary.minimum),
          money(summary.maximum))
      }
      val headers = Seq("Items", "Transactions", "Sum", "Average", "Min", "Max")
      s"T2:\n${formatTable(headers, rows)}"
    }
  }

  def formatCustomerCounts(step: CustomerCountStep): String = {
    if (step.counts.isEmpty) {
      s"${step.name}: no matching customers."
    } else {
      // sort by 1st element of the tuple (customerId)
      val rows = step.counts.sortBy(_._1).map { case (customerId, count) =>
        s"$customerId\t$count"
      }
      (Seq(s"${step.name}:", "CustID\tTransactions") ++ rows).mkString("\n")
    }
  }

  def formatCustomerComparisons(
      comparisons: Seq[CustomerCountComparison] = selectedCustomers
  ): String = {
    if (comparisons.isEmpty) {
      "T6: no matching customers."
    } else {
      val multiplier = comparisons.head.multiplier
      val rows = comparisons.sortBy(_.customerId).map { row =>
        Seq(row.customerId.toString, row.comparisonCount.toString,
          row.weightedComparisonCount.toString, row.baselineCount.toString)
      }
      val headers = Seq("CustID", "T5.count", s"T5.count * $multiplier", "T3.count")
      s"T6:\n${formatTable(headers, rows)}"
    }
  }

  private def money(amount: BigDecimal): String =
    amount.setScale(2, BigDecimal.RoundingMode.HALF_UP).bigDecimal.toPlainString
}