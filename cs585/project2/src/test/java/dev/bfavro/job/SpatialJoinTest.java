package dev.bfavro.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class SpatialJoinTest {

   /**
    * With Window [1,1]-[70,70]: verifies exact point/rectangle matches 
    * across a cell boundary, excludes outside points and a disjoint
    * rectangle. Ensures rectangle_id<TAB>x,y output.
    * 
    * Consider points exactly on rectangle edges (inclusive) and
    * one unit outside; domain corners; rectangles touching versus disjoint
    * from W; no-window, empty, and no-match inputs; and coordinates around
    * different CELL_SIZE boundaries.
    */
    @Test
    public void joinsMatchingPairsWithinOptionalWindow() throws Exception {
        Path testDirectory = Files.createTempDirectory("spatial-join-test");
        Path pointsFile = testDirectory.resolve("points.csv");
        Path rectanglesFile = testDirectory.resolve("rectangles.csv");
        Path outputDirectory = testDirectory.resolve("output");

        try {
            Files.write(pointsFile, Arrays.asList(
                "12,12",
                "50,50",
                "51,51",
                "54,54",
                "75,75", 
                "70,70"
            ), StandardCharsets.UTF_8);
            Files.write(rectanglesFile, Arrays.asList(
                "r1,10,10,5,5",
                "r2,48,48,30,30",
                "r3,70,70,5,5"
            ), StandardCharsets.UTF_8);

            Configuration configuration = new Configuration();
            configuration.set("mapreduce.framework.name", "local");
            configuration.set("fs.defaultFS", "file:///");
            configuration.setBoolean("spatial.window.enabled", true);
            configuration.setInt("spatial.window.x1", 1);
            configuration.setInt("spatial.window.y1", 1);
            configuration.setInt("spatial.window.x2", 70);
            configuration.setInt("spatial.window.y2", 70);

            Job job = Job.getInstance(configuration, "spatial join test");
            job.setJarByClass(SpatialJoin.class);
            MultipleInputs.addInputPath(
                job,
                new org.apache.hadoop.fs.Path(pointsFile.toUri()),
                TextInputFormat.class,
                SpatialJoin.PointMapper.class
            );
            MultipleInputs.addInputPath(
                job,
                new org.apache.hadoop.fs.Path(rectanglesFile.toUri()),
                TextInputFormat.class,
                SpatialJoin.RectangleMapper.class
            );
            job.setReducerClass(SpatialJoin.SpatialJoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(
                job,
                new org.apache.hadoop.fs.Path(outputDirectory.toUri())
            );

            assertTrue("Local Hadoop join job failed", job.waitForCompletion(true));

            List<String> outputLines;
            try (Stream<Path> outputFiles = Files.list(outputDirectory)) {
                Path partFile = outputFiles
                    .filter(path -> path.getFileName().toString().startsWith("part-r-"))
                    .findFirst()
                    .get();
                outputLines = Files.readAllLines(partFile, StandardCharsets.UTF_8);
            }

            Set<String> expected = new HashSet<>(Arrays.asList(
                "r1\t12,12",
                "r2\t50,50",
                "r2\t51,51",
                "r2\t54,54",
                "r2\t70,70",
                "r3\t70,70"
            ));
            assertEquals(6, outputLines.size());
            assertEquals(expected, new HashSet<>(outputLines));
        } finally {
            try (Stream<Path> paths = Files.walk(testDirectory)) {
                paths.sorted((left, right) -> right.compareTo(left))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
        }
    }
}