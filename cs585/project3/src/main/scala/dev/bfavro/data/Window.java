package dev.bfavro.data;

public class Window {

    private final int x1;
    private final int y1;
    private final int x2;
    private final int y2;

    public Window(int x1, int y1, int x2, int y2) {
        if (x1 > x2) {
            throw new IllegalArgumentException("x1 must be less than or equal to x2");
        }
        if (y1 > y2) {
            throw new IllegalArgumentException("y1 must be less than or equal to y2");
        }

        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
    }

    public int getX1() {
        return x1;
    }

    public int getY1() {
        return y1;
    }

    public int getX2() {
        return x2;
    }

    public int getY2() {
        return y2;
    }
}
