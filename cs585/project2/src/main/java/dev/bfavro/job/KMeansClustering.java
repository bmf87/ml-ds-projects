package dev.bfavro.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dev.bfavro.data.AggregationResult;
import dev.bfavro.data.Center;
import dev.bfavro.data.Point;
import dev.bfavro.data.SpatialDomain;
import dev.bfavro.util.KMeansUtils;

/**
 * Clusters input points into groups by repeatedly assigning each point to its
 * nearest center and moving each center to the mean of its assigned points.
 * This is the standard K-means heuristic for reducing within-cluster squared
 * distance; the result can depend on the initial center positions.
 *
 * In each MapReduce iteration, the mapper reads the current centers from the
 * distributed cache, assigns each point to its nearest center, and emits
 * {@code CenterID -> 1,x,y}. The combiner partially sums counts and coordinates
 * as {@code CenterID -> count,sumX,sumY} to reduce shuffle data. The reducer
 * aggregates those values, computes new mean coordinates, and reports whether
 * any center moved. Centers receiving no points retain their previous
 * coordinates, preserving the requested number of centers.
 *
 * After job completion, the driver writes the reducer's center records to
 * the next centers file and uses that file as the distributed-cache input for
 * the following iteration. Each iteration uses one reducer to update centers
 * and produce a single convergence status. The driver stops after two
 * consecutive unchanged iterations or the requested maximum number of
 * iterations.
 */
public class KMeansClustering {
    private static final String CENTER_CACHE_ALIAS = "centers-lkp";

    public static class PointMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private List<Center> centers;
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            centers = new ArrayList<>();
            Path centersPath = new Path(CENTER_CACHE_ALIAS);
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(centersPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    centers.add(new Center(
                        parts[0],
                        Double.parseDouble(parts[1]),
                        Double.parseDouble(parts[2])
                    ));
                }
            }
        }

        /**
         * Assigns the input point to its nearest cached center and emits
         * {@code CenterID -> 1,x,y} for aggregation.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            Point point = SpatialDomain.parsePoint(value.toString());
            Center nearestCenter = null;
            double minimumDistance = Double.MAX_VALUE;

            for (Center center : centers) {
                double distance = point.distanceTo(center.getX(), center.getY());
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    nearestCenter = center;
                }
            }

            if (nearestCenter != null) {
                outKey.set(nearestCenter.getId());
                outValue.set("1," + point.toString());
                context.write(outKey, outValue);
            }
        }
    }

    public static class CenterCombiner
        extends Reducer<Text, Text, Text, Text> {

        /**
         * Sums partial point counts and coordinate totals for one center.
         * Accepts mapper records and prior combiner aggregates so Hadoop may
         * invoke this aggregation zero or multiple times.
         */
        @Override
        protected void reduce(Text centerId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            AggregationResult aggregation = KMeansUtils.aggregate(values);
            context.write(centerId, new Text(
                aggregation.getCount() + "," +
                aggregation.getSumX() + "," +
                aggregation.getSumY()
            ));
        }
    }

    public static class CenterReducer
        extends Reducer<Text, Text, Text, Text> {

        private static final double EPSILON = 0.5;
        private Map<String, Center> oldCenters;
        private boolean centersChanged;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            oldCenters = new HashMap<>();
            Path centersPath = new Path(CENTER_CACHE_ALIAS);
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(centersPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    String id = parts[0];
                    oldCenters.put(id, new Center(
                        id,
                        Double.parseDouble(parts[1]),
                        Double.parseDouble(parts[2])
                    ));
                }
            }
        }

        /**
         * Computes the new mean for an assigned center and records whether it
         * moved. Unassigned centers remain in {@code oldCenters} for cleanup.
         */
        @Override
        protected void reduce(Text centerId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            AggregationResult aggregation = KMeansUtils.aggregate(values);
            long count = aggregation.getCount();
            long sumX = aggregation.getSumX();
            long sumY = aggregation.getSumY();
            double newX = (double) sumX / count;
            double newY = (double) sumY / count;

            Center oldCenter = oldCenters.remove(centerId.toString());
            if (oldCenter == null) {
                throw new IOException("Old center not found for ID: " + centerId);
            }

            if (Math.abs(oldCenter.getX() - newX) > EPSILON
                    || Math.abs(oldCenter.getY() - newY) > EPSILON) {
                centersChanged = true;
            }

            context.write(centerId, new Text(newX + "," + newY));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Center center : oldCenters.values()) {
                context.write(
                    new Text(center.getId()),
                    new Text(center.getX() + "," + center.getY())
                );
            }

            context.write(
                new Text("STATUS"),
                new Text(centersChanged ? "CHANGED" : "UNCHANGED")
            );
        }
    }

    private static boolean createNextCentersFile(FileSystem fs, Path iterationOutput,
        Path nextCenters) throws IOException {
        // Using single reducer
        Path reducerOutput = new Path(iterationOutput, "part-r-00000");
        boolean unchanged = false;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(reducerOutput)));
             BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(nextCenters, true)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\t", 2);
                if (parts.length != 2) {
                    continue;
                }

                String key = parts[0];
                String value = parts[1];
                if ("STATUS".equals(key)) {
                    unchanged = "UNCHANGED".equals(value);
                } else {
                    writer.write(key + "," + value);
                    writer.newLine();
                }
            }
        }

        return unchanged;
    }

    private static boolean runKMeansIteration(
        Configuration conf,
        Path centersInput,
        Path pointsInput,
        Path iterationOutput
    ) throws Exception {
        Job job = Job.getInstance(conf, "KMeans Clustering Iteration");
        job.setJarByClass(KMeansClustering.class);
        job.addCacheFile(new URI(
            centersInput.toUri().toString() + "#" + CENTER_CACHE_ALIAS
        ));
        FileInputFormat.addInputPath(job, pointsInput);
        job.setMapperClass(PointMapper.class);
        job.setCombinerClass(CenterCombiner.class);
        job.setReducerClass(CenterReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, iterationOutput);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        final int requiredUnchangedIterations = 2;
        if (args.length != 4) {
            System.err.println(
                "Usage: KMeansClustering <centers input> <points input> " +
                "<output directory> <max iterations>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path currentCenters = new Path(args[0]);
        Path pointsInput = new Path(args[1]);
        Path outputDirectory = new Path(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        int unchangedIterations = 0;

        for (int iteration = 1; iteration <= maxIterations; iteration++) {
            Path iterationOutput = new Path(outputDirectory, "iteration-" + iteration);
            Path nextCenters = new Path(outputDirectory, "centers-" + iteration + ".csv");

            System.out.println("Starting K-Means iteration " + iteration);
            boolean success = runKMeansIteration(
                conf, currentCenters, pointsInput, iterationOutput);
            if (!success) {
                System.err.println("K-Means iteration " + iteration + " failed.");
                System.exit(1);
            }

            boolean unchanged = createNextCentersFile(fs, iterationOutput, nextCenters);
            if (unchanged) {
                unchangedIterations++;
                System.out.println("Iteration " + iteration +
                    ": centers UNCHANGED (" + unchangedIterations + " consecutive)");
            } else {
                unchangedIterations = 0;
                System.out.println("Iteration " + iteration + ": centers CHANGED");
            }

            if (unchangedIterations >= requiredUnchangedIterations) {
                System.out.println("K-Means converged after " + iteration + " iterations.");
                break;
            }

            currentCenters = nextCenters;
        }
    }
}
