package dev.bfavro.model


/**
  * Model object representing a transaction made by a customer. A single transaction encapsulates 
  * all relevant information and includes details such as: the total amount, number of items, and a description.
  * Class encapsulate all relevant information about a single transaction.
  *
  * @param transId The unique identifier for the transaction.
  * @param custId The unique identifier for the customer.
  * @param transTotal The total amount of the transaction.
  * @param transNumItems The number of items in the transaction.
  * @param transDesc A description of the transaction.
  */
final case class Transaction(
    transId: Int,
    custId: Int,
    transTotal: BigDecimal,
    transNumItems: Int,
    transDesc: String
) extends Serializable


/**
  * Aggregate statistics for one group of transactions sharing the same number of items.
  * Produced by the T2 aggregation over the filtered T1 transactions; individual
  * transactions are not retained. Average transaction amount is derived from
  * totalSum / transactionCount and rounded only when displayed.
  *
  * @param itemCount The number of items shared by every transaction in this group.
  * @param transactionCount The number of transactions in this group.
  * @param totalSum The sum of transaction amounts in this group, before display rounding.
  * @param minimum The smallest transaction amount in this group.
  * @param maximum The largest transaction amount in this group.
  */
final case class ItemCountStats(
    itemCount: Int,
    transactionCount: Long,
    totalSum: BigDecimal,
    minimum: BigDecimal,
    maximum: BigDecimal
) extends Serializable

/** A named customer-count report for one processing step, such as T3 or T5. */
final case class CustomerCountStep(
  name: String,
  counts: Seq[(Int, Long)]
)

/** Counts and multiplier used to evaluate one customer's T6 comparison. */
final case class CustomerCountComparison(
    customerId: Int,
    baselineCount: Long,
    comparisonCount: Long,
    multiplier: Long
) extends Serializable {
  def weightedComparisonCount: BigInt = BigInt(comparisonCount) * multiplier
}