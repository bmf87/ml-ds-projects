package dev.bfavro.util;

import dev.bfavro.data.AggregationResult;
import org.apache.hadoop.io.Text;

public class KMeansUtils {

    private KMeansUtils() {
    }

    public static AggregationResult aggregate(Iterable<Text> values) {
        int count = 0;
        long sumX = 0;
        long sumY = 0;

        for (Text value : values) {
                // parse count,x,y
                String[] parts = value.toString().split(",");
                int countPart = Integer.parseInt(parts[0]);
                // Changed to long
                long x = Long.parseLong(parts[1]);
                long y = Long.parseLong(parts[2]);
                // aggregate count and coordinates
                count += countPart;
                sumX += x;
                sumY += y;
            }

        return new AggregationResult(count, sumX, sumY);
    }
        

}
