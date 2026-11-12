package dev.bfavro.output

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{Closeable, IOException}
import java.nio.charset.StandardCharsets

/**
  * Writes formatted step results to one file in driver call order.
  * Create one instance per job and reuse it for all steps, passing strings from
  * Result's formatting methods to saveToHdfs. The path uses the supplied Hadoop configuration,
  * so it can refer to HDFS or another Hadoop-supported filesystem.
  *
  * Creates a new file without overwriting existing results. The same stream remains
  * open across calls; this does not require filesystem append support and does not
  * append to files from previous runs. Each call writes UTF-8 text, ensures a trailing
  * newline, and flushes the output. Close the writer in finally, including on failure.
  * Only use this writer on the driver, not inside distributed RDD operations.
  *
  * @param outputPath The output file path or URI, not a Spark output directory.
  * @param configuration The driver's Hadoop configuration, such as sc.hadoopConfiguration.
  */
final class StepWriter(outputPath: String, configuration: Configuration) extends Closeable {
  private val path = new Path(outputPath)
  private val output = path.getFileSystem(configuration).create(path, false)
  private var closed = false

  /**
    * Writes the next formatted block to the file opened by this writer.
    * Earlier blocks are preserved in call order; filesystem errors propagate.
    *
    * @param text The formatted step result, potentially containing multiple lines.
    */
  def saveToHdfs(text: String): Unit = {
    if (closed) throw new IOException("Cannot write using a closed StepWriter")
    output.write(text.getBytes(StandardCharsets.UTF_8))
    if (!text.endsWith("\n")) output.write('\n')
    output.flush()
    output.hflush()
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      output.close()
    }
  }
}