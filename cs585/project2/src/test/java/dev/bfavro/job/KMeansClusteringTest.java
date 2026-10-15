package dev.bfavro.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KMeansClusteringTest {

    private Path testDirectory;

    @Before
    public void createTestDirectory() throws IOException {
        testDirectory = Files.createTempDirectory("kmeans-clustering-test");
    }

    @After
    public void deleteTestDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(testDirectory)) {
            for (Path path : paths.sorted(Collections.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    @Test
    public void preservesCenterThatReceivesNoPoints() throws Exception {
        Path centersFile = testDirectory.resolve("centers.csv");
        Path pointsFile = testDirectory.resolve("points.csv");
        Path outputDirectory = testDirectory.resolve("output");
        Files.write(centersFile, Arrays.asList("C0,0,0", "C1,1000,1000"), StandardCharsets.UTF_8);
        Files.write(pointsFile, Arrays.asList("1,1", "2,2", "3,3"), StandardCharsets.UTF_8);

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("fs.defaultFS", "file:/");

        Job job = Job.getInstance(configuration, "k-means empty-center test");
        job.setJarByClass(KMeansClustering.class);
        job.addCacheFile(new URI(centersFile.toUri().toString() + "#centers-lkp"));
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(pointsFile.toUri()));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(outputDirectory.toUri()));
        job.setMapperClass(KMeansClustering.PointMapper.class);
        job.setCombinerClass(KMeansClustering.CenterCombiner.class);
        job.setReducerClass(KMeansClustering.CenterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        assertTrue("Local Hadoop K-means job failed", job.waitForCompletion(true));

        List<String> outputLines;
        try (Stream<Path> outputFiles = Files.list(outputDirectory)) {
            Path partFile = outputFiles
                .filter(path -> path.getFileName().toString().startsWith("part-r-"))
                .findFirst()
                .get();
            outputLines = Files.readAllLines(partFile, StandardCharsets.UTF_8);
        }

        assertEquals(3, outputLines.size());
        assertEquals(new HashSet<>(Arrays.asList(
            "C0\t2.0,2.0",
            "C1\t1000.0,1000.0",
            "STATUS\tCHANGED"
        )), new HashSet<>(outputLines));
    }
}
