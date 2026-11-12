package dev.bfavro.job

import dev.bfavro.data.CellGrid._
import dev.bfavro.job.GridCellJob._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class GridCellJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext(new SparkConf()
      .setAppName("GridCellJobSpec")
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

  test("cell IDs are 1-based, row-major from the bottom-left") {
    assert(cellId(1, 1) == 1)
    assert(cellId(20, 20) == 1)
    assert(cellId(21, 1) == 2)
    assert(cellId(1, 21) == 501)
    assert(cellId(10000, 10000) == 250000)
  }

  test("neighbors match the assignment examples and boundaries") {
    assert(neighbors(1).sorted == Seq(2, 501, 502))
    assert(neighbors(1002).sorted == Seq(501, 502, 503, 1001, 1003, 1501, 1502, 1503))
    assert(neighbors(500).size == 3)
    assert(neighbors(250000).size == 3)
    assert(neighbors(2).size == 5)
  }

  test("parser rejects malformed and out-of-domain rows") {
    assert(parseCellId("21,21").contains(502))
    assert(parseCellId("0,5").isEmpty)
    assert(parseCellId("10001,5").isEmpty)
    assert(parseCellId("abc").isEmpty)
  }

  test("relative-density index uses own count over the average neighbor count") {
    // Cell 1 has 6 points; neighbors 2, 501, 502 have 1, 2, 0 -> avg 1, I = 6.
    val ids = Seq.fill(6)(1) ++ Seq(2) ++ Seq.fill(2)(501)
    val byId = computeDensityIndex(sc.parallelize(ids, 2)).collect().map(c => c.cellId -> c).toMap
    assert(byId(1).index == 6.0)
    assert(byId(502).count == 0L && byId(502).index == 0.0)
    // Cell 2: own 1, neighbors (1, 3, 501, 502, 503) sum 8 over 5 -> 1.6.
    assert(math.abs(byId(2).index - 1 / 1.6) < 1e-12)
    val top = computeDensityIndex(sc.parallelize(ids, 2)).takeOrdered(2)(ByIndexDescending)
    assert(top.map(_.cellId).toSeq == Seq(1, 501))
  }

  test("isolated non-empty cell has infinite index") {
    val result = computeDensityIndex(sc.parallelize(Seq(1002))).collect().find(_.cellId == 1002).get
    assert(result.index.isPosInfinity)
  }

  test("run includes the relative-density indices of each top cell's neighbors") {
    val input = Files.createTempFile("grid-cell-neighbor-index", ".csv")
    try {
      Files.write(input, "1,1\n1,1\n21,1\n".getBytes(StandardCharsets.UTF_8))
      val result = GridCellJob.run(sc, input.toUri.toString, topK = 2)
      val topById = result.topCells.map(cell => cell.cellId -> cell).toMap
      val firstCellNeighbors = result.neighborsByCell(1).map(n => n.cellId -> n.index).toMap
      val secondCellNeighbors = result.neighborsByCell(2).map(n => n.cellId -> n.index).toMap

      assert(topById.keySet == Set(1, 2))
      assert(firstCellNeighbors(2).contains(topById(2).index))
      assert(firstCellNeighbors(501).contains(0.0))
      assert(secondCellNeighbors(1).contains(topById(1).index))
      assert(format(result).contains("NeighborRelativeDensityIndex"))
      assert(format(result).contains("2.500000"))
    } finally {
      Files.deleteIfExists(input)
    }
  }
}
