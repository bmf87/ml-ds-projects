package dev.bfavro.data;

public class Point {
    private final int x;
    private final int y;

    public Point(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() { 
        return x;
    }
    
    public int getY() { 
        return y;
    }

    /**
     * Calculate Euclidean distance to another point.
     * 
     * @param other The other point to which the distance is calculated.
     * @return The Euclidean distance between this point and the other point.
    */
    public double distanceTo(Point other) {
        return distanceTo(other.x, other.y);
    }

    /**
     * Calculates Euclidean distance to coordinates that may be fractional.
     * {@link Math#hypot(double, double)} computes sqrt(dx*dx + dy*dy),
     * avoiding intermediate overflow or underflow without rounding coordinates to integers.
     *
     * @param otherX the target x coordinate
     * @param otherY the target y coordinate
     * @return the distance from this point to the target coordinates
     */
    public double distanceTo(double otherX, double otherY) {
        return Math.hypot(this.x - otherX, this.y - otherY);
    }
    
    @Override
    public String toString() {
        return x + "," + y;
    }
}
