package dev.bfavro.data;

public class AggregationResult {
    private final long count;
    private final long sumX;
    private final long sumY;

    public AggregationResult(long count, long sumX, long sumY) {
        this.count = count;
        this.sumX = sumX;
        this.sumY = sumY;
    }

    public long getCount() {
        return count;
    }

    public long getSumX() {
        return sumX;
    }

    public long getSumY() {
        return sumY;
    }

}
