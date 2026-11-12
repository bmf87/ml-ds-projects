package dev.bfavro.job

import dev.bfavro.data.CellGrid.{CellSize, CellsPerSide, neighbors, parseCellId}
import dev.bfavro.output.{StepWriter, TextTable}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Reports the grid cells with the highest relative-density index I(X) = X.count / avg(N(X).count).
  * Grid layout and cell ID scheme are defined in [[dev.bfavro.data.CellGrid]].
  */
object GridCellJob {
  val DefaultTopK: Int = 50

  final case class CellDensity(cellId: Int, count: Long, neighborAverage: Double, index: Double)

  final case class NeighborDensity(cellId: Int, index: Option[Double])

  final case class GridCellResult(
      invalidCount: Long,
      topCells: Seq[CellDensity],
      neighborsByCell: Map[Int, Seq[NeighborDensity]]
  )

  /**
    * Computes the relative-density index I(X) = X.count / Average(Y1.count, ..., Yn.count)
    * for each cell X, where Y1..Yn are its 3, 5 or 8 neighbors (empty neighbors count as 0).
    *
    * Distributed in two shuffles: count points per cell, then send each count to the cell
    * itself and to every neighbor, so each cell receives (own count, neighbor sum).
    *
    * Cases:
    *  - own > 0, neighborSum > 0: occupied cell; occupied neighborhood -> finite positive index.
    *  - own = 0, neighborSum > 0: empty cell; occupied neighborhood -> index 0 (an empty cell next to points).
    *  - own > 0, neighborSum = 0: occupied cell; no neighborhood -> A positive count over an empty 
    *       neighborhood is treated as +Infinity.
    *  - own = 0, neighborSum = 0: 0/0 is undefined; such cells never emitted by flatMap.
    */
  def computeDensityIndex(cellIds: RDD[Int]): RDD[CellDensity] = {
    // MapReduce Pattern: emit points per cell: (cellId, 1)
    // ReduceByKey (cellId) to get total count per cell
    val counts = cellIds
        .map(cellId => (cellId, 1L))
        .reduceByKey((partialCountA, partialCountB) => partialCountA + partialCountB)
    
    // flatMap: send counts to cellId (own) and each neighbor
    // For each cell, emit (cellId, (ownCount, neighborSum)) for itself & neighbor
    // e.g. (1002, (4, 0)) for own, (neighborId, (0, 4)) for neighbors
    counts
      .flatMap { case (id, n) => 
        (id, (n, 0L)) +: neighbors(id).map(nb => (nb, (0L, n)))
      }
      // Sum own and neighbor pairs (a, b) into => cellId(ownCount, neighborSum)
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)) 
      // Compute relative-density index for each cell
      .map { case (id, (own, neighborSum)) =>
        val neighborCount = neighbors(id).size
        val rDensityIndex =
          if (neighborSum == 0L) Double.PositiveInfinity
          else own.toDouble / (neighborSum.toDouble / neighborCount)
        // Agg output: cellID, count, neighbor average, I(X)
        CellDensity(id, own, neighborSum.toDouble / neighborCount, rDensityIndex)
      }
  }

  // Orders higher relative-density indices first; ties go to the lower cell ID
  val ByIndexDescending: Ordering[CellDensity] =
    Ordering.by[CellDensity, (Double, Int)](c => (-c.index, c.cellId))

  // Orders higher neighbor relative-density indices first; ties go to the lower cell ID
  val ByNeighborIndexDescending: Ordering[NeighborDensity] =
    Ordering.by[NeighborDensity, (Int, Double, Int)] { neighbor =>
    neighbor.index match {
      case Some(index) => (0, -index, neighbor.cellId)
      // Missing index (undefined) treated as lowest priority in ordering
      case None        => (1, 0.0, neighbor.cellId)
    }
  }

  def run(sc: SparkContext, inputPath: String, topK: Int = DefaultTopK): GridCellResult = {
    require(topK > 0, "Top-k must be a positive integer")
    val parsed = sc.textFile(inputPath).map(parseCellId).persist(StorageLevel.MEMORY_AND_DISK)
    val densities = computeDensityIndex(parsed.flatMap(_.toList)).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val invalidCount = parsed.filter(_.isEmpty).count()
      val top = densities.takeOrdered(topK)(ByIndexDescending).toSeq
      val neighborRequests = sc.parallelize(
        // Get neighbors: for each top cell, create a pair (neighborId, topCellId) to request neighbor densities
        top.flatMap(cell => neighbors(cell.cellId).map(neighborId => (neighborId, cell.cellId))),
        math.max(1, sc.defaultParallelism))
      // Join neighborRequests with `densities` -> produces (neighborId, (topKCellId, neighborIndex)) pairs
      // map reshapes into (topCellId, NeighborDensity) 
      val neighborDensities = neighborRequests
        .leftOuterJoin(densities.map(cell => (cell.cellId, cell.index)))
        .map { case (neighborId, (topCellId, index)) =>
          (topCellId, NeighborDensity(neighborId, index))
        }
        .collect()
        .groupBy { case (topCellId, _) => topCellId }
        .map { case (topCellId, entries) =>
          val sortedNeighbors = entries
            .map { case (_, neighborDensity) => neighborDensity }
            .sorted(ByNeighborIndexDescending)
            .toSeq
          topCellId -> sortedNeighbors
        }
      GridCellResult(invalidCount, top, neighborDensities)
    } finally {
      // Unpersist cached RDDs to free memory
      densities.unpersist(blocking = true)
      parsed.unpersist(blocking = true)
    }
  }

  def format(result: GridCellResult): String = {
    val header =
      s"Top ${result.topCells.size} grid cells by relative-density index " +
        s"(${CellsPerSide}x$CellsPerSide grid of ${CellSize}x$CellSize cells, " +
        "IDs row-major from 1 at bottom-left)\n" +
        s"Invalid point rows skipped: ${result.invalidCount}\n"
    val headers = Seq("Rank", "(CellID, RelativeDensityIndex)", "Count", "NeighborAverage")
    val rows = result.topCells.zipWithIndex.map { case (c, i) =>
      Seq((i + 1).toString, f"(${c.cellId}, ${c.index}%.6f)", c.count.toString,
        f"${c.neighborAverage}%.4f")
    }
    val neighborHeaders = Seq("TopCellID", "NeighborCellID", "NeighborRelativeDensityIndex")
    val neighborRows = result.topCells.flatMap { cell =>
      result.neighborsByCell.getOrElse(cell.cellId, Seq.empty).map { neighbor =>
        Seq(cell.cellId.toString, neighbor.cellId.toString,
          neighbor.index.map(index => f"$index%.6f").getOrElse("undefined"))
      }
    }
    header + TextTable.formatTable(headers, rows) +
      "\n\nNeighbor relative-density indices:\n" + TextTable.formatTable(neighborHeaders, neighborRows)
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 1 && args.length <= 3,
      "Usage: GridCellJob <input-path-or-URI> [top-k] optional:[output-file-or-URI]")
    val topK = if (args.length >= 2) {
      Try(args(1).toInt).getOrElse(
        throw new IllegalArgumentException("Top-k must be a positive integer"))
    } else DefaultTopK
    require(topK > 0, "Top-k must be a positive integer")

    val sc = new SparkContext(new SparkConf().setAppName("GridCell Job"))
    try {
      val stepWriter = args.lift(2).map(path => new StepWriter(path, sc.hadoopConfiguration))
      try {
        val text = format(run(sc, args(0), topK))
        println(text)
        stepWriter.foreach(_.saveToHdfs(text))
      } finally {
        stepWriter.foreach(_.close())
      }
    } finally {
      sc.stop()
    }
  }
}
