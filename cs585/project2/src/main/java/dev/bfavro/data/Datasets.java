package dev.bfavro.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Random;


public class Datasets {
    private static final long TARGET_SIZE_BYTES = 100L * 1024 * 1024;

    private static int randomCoordinate(Random random) {
        return random.nextInt(
            SpatialDomain.MAX_COORDINATE - SpatialDomain.MIN_COORDINATE + 1
        ) + SpatialDomain.MIN_COORDINATE;
    }

    public static void create2DPoints(String outputFile,
                                      long targetBytes,
                                      long seed) throws IOException {

        Random random = new Random(seed);
        long bytesWritten = 0;

        try (BufferedWriter writer = Files.newBufferedWriter(
                                        Paths.get(outputFile),
                                        StandardCharsets.UTF_8)) {

            while (bytesWritten < targetBytes) {
                int x = randomCoordinate(random);
                int y = randomCoordinate(random);
                // Write conceptual pts as CSV line 
                String line = x + "," + y + "\n";
                writer.write(line);
                // Could use line.length() for getting bytes
                // getBytes is more accurate for multibyte UTF-8 characters
                bytesWritten += line.getBytes(StandardCharsets.UTF_8).length;
            }
        }
    }

    // Count-based overload for use with Centroids
    public static void create2DPoints(String outputFile, int pointCount, long seed)
        throws IOException {
        create2DPoints(outputFile, pointCount, seed, null);
    }

    public static void create2DPoints(String outputFile, int pointCount, long seed, String prefix)
        throws IOException {
        if (pointCount < 0) {
            throw new IllegalArgumentException("pointCount must be non-negative");
        }
        String rowPrefix = null; 
        Random random = new Random(seed);

        try (BufferedWriter writer = Files.newBufferedWriter(
            Paths.get(outputFile), StandardCharsets.UTF_8)) {
            for (int i = 0; i < pointCount; i++) {
                rowPrefix = prefix == null || prefix.isEmpty() ? "" : prefix + i + ",";
                String line = rowPrefix + randomCoordinate(random) + "," + randomCoordinate(random) + "\n";
                writer.write(line);
            }
        }
    }

    // Generate random Point within: 1..10000
    private static Point randomPoint(Random random) {
        return new Point(
            randomCoordinate(random), 
            randomCoordinate(random)
        );
    }

    // Produces 1..20 inclusive: [1,20]
    private static int randomHeight(Random random) {
        return random.nextInt(20) + 1;
    }

    // Produces 1..7 inclusive: [1,7]
    private static int randomWidth(Random random) {
        return random.nextInt(7) + 1;
    }

    // Ensure rectangle fits in the spatial domain
    private static boolean fitsInSpace(Point bottomLeft,
                                       int height,
                                       int width) {

        return bottomLeft.getX() + width <= SpatialDomain.MAX_COORDINATE
            && bottomLeft.getY() + height <= SpatialDomain.MAX_COORDINATE;
    }

    public static void create2DRectangles(String outputFile,
                                          long targetBytes,
                                          long seed) throws IOException {

        Random random = new Random(seed);
        long bytesWritten = 0;

        try (BufferedWriter writer = Files.newBufferedWriter(
                                        Paths.get(outputFile),
                                        StandardCharsets.UTF_8)) {

            int r_id = 0;
            while (bytesWritten < targetBytes) {
                Point bottomLeft = randomPoint(random);
                int height = randomHeight(random);
                int width = randomWidth(random);

                if (!fitsInSpace(bottomLeft, height, width)) {
                    continue;
                }

                // Write x,y pts and h,w as CSV line 
                String line = r_id + "," + bottomLeft.getX() + "," + bottomLeft.getY() + "," + height + "," + width + "\n";
                writer.write(line);
                bytesWritten += line.getBytes(StandardCharsets.UTF_8).length;
                r_id++;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // Seeds for reproducibility
        long p_seed = 12345L;
        long r_seed = 67890L;
        
        create2DPoints("data/datasetP.csv", TARGET_SIZE_BYTES, p_seed);
        create2DRectangles("data/datasetR.csv", TARGET_SIZE_BYTES, r_seed);
    }
}
