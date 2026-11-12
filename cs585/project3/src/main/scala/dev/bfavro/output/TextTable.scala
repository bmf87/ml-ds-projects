package dev.bfavro.output

/**
  * Formats headers and rows as a plain-text table, padding columns to fit their contents.
  * Shared by job result formatters.
  */
object TextTable {
  // Add Pretty Print Capability: size columns to fit headers and values
  def formatTable(headers: Seq[String], rows: Seq[Seq[String]]): String = {
    val widths = headers.indices.map { columnIndex =>
      (headers(columnIndex).length +: rows.map(_(columnIndex).length)).max
    }
    (headers +: rows).map(values => formatRow(values, widths)).mkString("\n")
  }

  // Pads columns to their widths with two-space gaps, leaving last column unpadded
  private def formatRow(values: Seq[String], widths: Seq[Int]): String =
    values.zipWithIndex.map { case (value, columnIndex) =>
      if (columnIndex == widths.size - 1) value  // unchanged
      else value.padTo(widths(columnIndex), ' ') // add spaces til column width
    }.mkString("  ")
}
