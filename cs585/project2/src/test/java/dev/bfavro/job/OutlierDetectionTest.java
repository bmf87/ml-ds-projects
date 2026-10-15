package dev.bfavro.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import dev.bfavro.data.SpatialDomain;

public class OutlierDetectionTest {

    private Path testDirectory;

    @Before
    public void createTestDirectory() throws IOException {
        testDirectory = Files.createTempDirectory("outlier-detection-test");
    }

    @After
    public void deleteTestDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(testDirectory)) {
            for (Path path : paths.sorted(Collections.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    /**
     * At r=5, k=2: test verifies the following:
     *  - cell-boundary replication
     *  - distinct IDs at duplicate coordinates,
     *  - inclusion of points exactly r apart
     *  - isolated outliers
     * 
     * Lower r or raise k to make the three-point cluster fail.
     * Set k=1 to stop flagging the duplicate pair.
     */ 
    @Test
    public void reportsOutliersAcrossCellBoundariesDuplicatesAndInclusiveRadius() throws Exception {
        int r = 5;
        int k = 2;
        List<String> outliers = runJob(r, k, Arrays.asList(
            "48,10", "50,10", "52,10",        // straddles the x=50/51 cell boundary; none are outliers
            "200,200", "200,200",                  // same coordinates: each counts the other, not itself
            "1000,1000", "1003,1004", "1000,1005", // pairs at exactly distance r count as neighbors
            "500,500",                             // isolated
            "1,1"                                  // isolated at the domain corner
        ));

        assertEquals(Arrays.asList(
            "1,1\t0",
            "200,200\t1",
            "200,200\t1",
            "500,500\t0"
        ), outliers);
    }

    /**
     * At r=30, k=2 gives cellSize=60 (outlier.cellSize = 2 * r); the cluster has two 
     * neighbors per point and the isolated point is an outlier. 
     * 
     * Try k=3 to flag the cluster, or r<=25 to exercise the default 
     * 50-unit grid (and adjust expected output).
     */
    @Test
    public void usesConfiguredCellSizeWhenRadiusExceedsDefaultHalfCell() throws Exception {
        int r = 30;                      
        int k = 2;
        List<String> outliers = runJob(r, k, Arrays.asList(
            "55,55", "70,55", "85,55",           // homes span cells 0 and 1 when cellSize = 60
            "500,500"
        ));

        assertEquals(Collections.singletonList("500,500\t0"), outliers);
    }

    private List<String> runJob(int r, int k, List<String> points) throws Exception {
        Path pointsFile = testDirectory.resolve("points.csv");
        Path outputDirectory = testDirectory.resolve("output");
        Files.write(pointsFile, points, StandardCharsets.UTF_8);

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("outlier.r", r);
        conf.setInt("outlier.k", k);
        conf.setInt("outlier.cellSize", Math.max(SpatialDomain.CELL_SIZE, 2 * r));

        Job job = Job.getInstance(conf, "outlier detection test");
        job.setJarByClass(OutlierDetection.class);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(pointsFile.toUri()));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(outputDirectory.toUri()));
        job.setMapperClass(OutlierDetection.PointMapper.class);
        job.setReducerClass(OutlierDetection.OutlierDetectionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        assertTrue("Local Hadoop outlier job failed", job.waitForCompletion(true));

        List<String> lines = new ArrayList<>();
        try (Stream<Path> files = Files.list(outputDirectory)) {
            for (Path part : files
                    .filter(path -> path.getFileName().toString().startsWith("part-r-"))
                    .collect(Collectors.toList())) {
                lines.addAll(Files.readAllLines(part, StandardCharsets.UTF_8));
            }
        }
        Collections.sort(lines);
        return lines;
    }
}
