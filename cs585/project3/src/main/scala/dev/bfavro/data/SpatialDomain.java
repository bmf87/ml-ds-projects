package dev.bfavro.data;

import java.util.ArrayList;
import java.util.List;


public final class SpatialDomain {
    public static final int MIN_COORDINATE = 1;
    public static final int MAX_COORDINATE = 10000;
    // Size of each grid cell for spatial partitioning
    // Smaller cells produce higher key counts (distribute load more evenly)
    public static final int CELL_SIZE = 50;

    private SpatialDomain() {
    }

     // Parses line from datasetP.csv into Point domain object
    public static Point parsePoint(String line) {
        String[] parts = line.split(",");

        int x = Integer.parseInt(parts[0].trim());
        int y = Integer.parseInt(parts[1].trim());

        return new Point(x, y);
    }

    // Parses line from datasetR.csv into Rectangle domain object
    public static Rectangle parseRectangle(String line) {  
        String[] parts = line.split(",");

        String id = parts[0].trim();
        int x = Integer.parseInt(parts[1].trim());
        int y = Integer.parseInt(parts[2].trim());
        int height = Integer.parseInt(parts[3].trim());
        int width = Integer.parseInt(parts[4].trim());

        Point bottomLeft = new Point(x, y);
        return new Rectangle(id, bottomLeft, height, width);
    }

    // Determines the grid cell ID for a Point
    public static String getCellId(Point p) {
        long cellX = (long) ((p.getX() - 1) / CELL_SIZE); 
        long cellY = (long) ((p.getY() - 1) / CELL_SIZE);
        return cellX + "_" + cellY;
    }

    // Determines the grid cell ID for a Point using tuned cellSize
    public static String getCellId(Point p, int cellSize) {
        long cellX = (long) ((p.getX() - 1) / cellSize); 
        long cellY = (long) ((p.getY() - 1) / cellSize);
        return cellX + "_" + cellY;
    }
    
    public static List<String> getOverlappingCells(Rectangle r) {
        return getOverlappingCells(r.getLeft(), r.getBottom(), r.getRight(), r.getTop(), CELL_SIZE);
    }

    /**
     * Returns a list of grid cell IDs that overlap with the given window.
     * The cellSize parameter specifies the size of each grid cell.
     */
    public static List<String> getOverlappingCells(Window w, int cellSize) {
        return getOverlappingCells(w.getX1(), w.getY1(), w.getX2(), w.getY2(), cellSize);
    }

    //  Guardrail/Clamp on cells: ensures window coordinates lie within the spatial domain boundaries
    //  Bounds are inclusive within MIN_COORDINATE..MAX_COORDINATE.
    public static List<String> getOverlappingCells(int minX, int minY, int maxX, int maxY, int cellSize) {
        List<String> cells = new ArrayList<>();
        // Convert inclusive left/right coordinates to the first and last X cell indexes.
        // Subtracting 1 maps coordinates 1..CELL_SIZE to cell 0, CELL_SIZE+1..2*CELL_SIZE to cell 1, etc...
        int minCellX = (minX - 1) / cellSize;
        int maxCellX = (maxX - 1) / cellSize;
        // Convert inclusive bottom/top coordinates to get the Y cell range.
        int minCellY = (minY - 1) / cellSize;
        int maxCellY = (maxY - 1) / cellSize;

        // Enumerate pair names overlapped by the rectangle
        // Visits every X/Y cell-index pair in the inclusive ranges
        for (int cellX = minCellX; cellX <= maxCellX; cellX++) {
            for (int cellY = minCellY; cellY <= maxCellY; cellY++) {
                cells.add(cellX + "_" + cellY);
            }
        }

        return cells;
    }

}