package dev.bfavro.data;
public class Rectangle {

    private final String id;
    private final Point bottomLeft;
    private final int height;
    private final int width;

    public Rectangle(String id, Point bottomLeft, int height, int width) {
        this.id = id;
        this.bottomLeft = bottomLeft;
        this.height = height;
        this.width = width;
    }

    /**
     *  Identifier for the rectangle
     *  Not a geometric property
     **/
    public String id() {
        return id;
    }

    public Point getBottomLeft() {
        return bottomLeft;
    }

    public int getHeight() {
        return height;
    }

    public int getWidth() {
        return width;
    }

    public int getLeft() {
        return bottomLeft.getX();
    }

    public int getBottom() {
        return bottomLeft.getY();
    }

    public int getRight() {
        return bottomLeft.getX() + width;
    }

    public int getTop() {
        return bottomLeft.getY() + height;
    }

    public boolean contains(Point p) {
        return p.getX() >= getLeft()
            && p.getX() <= getRight()
            && p.getY() >= getBottom()
            && p.getY() <= getTop();
    }
}
