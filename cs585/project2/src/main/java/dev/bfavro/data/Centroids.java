package dev.bfavro.data;

import java.io.IOException;

public class Centroids {

    public static void createCentroids(String outputFile, int pointCount, long seed, String prefix) throws IOException {
        Datasets.create2DPoints(outputFile, pointCount, seed, prefix);
    }

    public static void main(String[] args) throws IOException {
        // Seed for reproducibility
        long c_seed = 12345L;
        int pointCount = args.length > 0 ? Integer.parseInt(args[0]) : 10;
       
        createCentroids("data/centers.csv", pointCount, c_seed, "C");
    }

}
