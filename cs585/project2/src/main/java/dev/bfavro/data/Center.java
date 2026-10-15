package dev.bfavro.data;

/**
 * Represents a center in the KMeans clustering algorithm.
 * Contains the center's ID and its coordinates (x, y).
 * 
 * Used double precision for x, y coordinates to support fractional values.
 */

public class Center {

    private final String id;
    private final double x;
    private final double y;

    public Center(String id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y; 
    }

    public String getId() {
        return id;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

}
