package dev.bfavro.data

import scala.util.Try

/**
  * Fixed 500x500 grid of 20x20 cells over the 1..10,000 domain.
  *
  * Cell IDs are 1-based and row-major starting at the bottom-left cell:
  * col = (x - 1) / 20, row = (y - 1) / 20, id = row * 500 + col + 1.
  * Conversely row = (id - 1) / 500 and col = (id - 1) % 500.
  */
object CellGrid {
  // Inclusive bounds shared by both axes (domain is square)
  val MinCoordinate: Int = SpatialDomain.MIN_COORDINATE
  val MaxCoordinate: Int = SpatialDomain.MAX_COORDINATE
  // Side length of each square cell, in coordinate units
  val CellSize: Int = 20
  // The grid is CellsPerSide x CellsPerSide (500 x 500 = 250,000 cells)
  val CellsPerSide: Int = (MaxCoordinate - MinCoordinate + 1) / CellSize

  // True if a single x or y coordinate lies inside the domain
  def inDomain(v: Int): Boolean = v >= MinCoordinate && v <= MaxCoordinate

  /**
    * Maps a point to its 1-based, row-major cell ID: `row * CellsPerSide + col + 1`.
    * Coordinates are shifted by 1 so that 1..20 fall in row/column index 0.
    *
    * @return a cell ID in 1..CellsPerSide², assuming `x` and `y` are in the domain.
    */
  def cellId(x: Int, y: Int): Int =
    ((y - 1) / CellSize) * CellsPerSide + (x - 1) / CellSize + 1

  // 0-based row (y index) of a cell; row 0 is the bottom edge
  def row(id: Int): Int = (id - 1) / CellsPerSide

  // 0-based column (x index) of a cell; column 0 is the left edge
  def col(id: Int): Int = (id - 1) % CellsPerSide

  // IDs of the up-to-8 cells sharing an edge or corner with `id`
  // Boundary cells have 3 or 5 neighbors
  def neighbors(id: Int): Seq[Int] = {
    val r0 = row(id)
    val c0 = col(id)
    for {
      r <- r0 - 1 to r0 + 1
      c <- c0 - 1 to c0 + 1
      if (r != r0 || c != c0) && r >= 0 && r < CellsPerSide && c >= 0 && c < CellsPerSide
    } yield r * CellsPerSide + c + 1
  }

  // Cell ID for an "x,y" CSV row, or None if the row is malformed or outside the domain
  def parseCellId(line: String): Option[Int] =
    Try(SpatialDomain.parsePoint(line)).toOption.collect {
      case p if inDomain(p.getX) && inDomain(p.getY) => cellId(p.getX, p.getY)
    }
}
