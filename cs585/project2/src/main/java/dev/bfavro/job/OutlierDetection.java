package dev.bfavro.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import dev.bfavro.data.Window;
import dev.bfavro.data.Point;
import dev.bfavro.data.SpatialDomain;



/**
 * Detects distance-based outliers: a point p is an outlier if fewer than k
 * other points lie within distance r of p (inclusive). Radius r and threshold
 * k are supplied as job arguments.
 *
 * The job partitions the space into a uniform grid so each reducer can decide
 * outlier status using only the points in one cell. Each point is sent to its
 * home cell as a core record and replicated as a support record to every other
 * cell its radius-r neighborhood window overlaps. Because neighborhoods are
 * symmetric, every point within r of a core point reaches that core point's
 * home cell, so the reducer sees each core point's complete neighborhood (C|
 *  and S| tags indicate core and support records, respectively).
 *
 * The reducer evaluates only core points, counting neighbors among all core
 * and support records in the cell, so each point is judged exactly once.
 * Cell size trades replication against per-cell comparisons: cells at least
 * 2r wide limit each point to at most four copies, while larger cells increase
 * the number of distance checks per reducer.
 */
public class OutlierDetection {

    public static class PointMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private int r;
        private int cellSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            r = context.getConfiguration().getInt("outlier.r", -1);
            if (r < 0) {
                throw new IllegalStateException("outlier.r not set");
            }
            cellSize = context.getConfiguration().getInt("outlier.cellSize", SpatialDomain.CELL_SIZE);
            if (cellSize < 1) {
                throw new IllegalStateException("outlier.cellSize not set or invalid");
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                        
            Point p = SpatialDomain.parsePoint(value.toString());
            
            // Each split reports byte offset from start of whole file
            // Giving us a unique identifier for the point!
            long id = key.get();
            String home = SpatialDomain.getCellId(p, cellSize);
            // Clamp the window to lie within the spatial domain boundaries
            Window nbr = new Window(
                Math.max(SpatialDomain.MIN_COORDINATE, p.getX() - r), Math.max(SpatialDomain.MIN_COORDINATE, p.getY() - r),
                Math.min(SpatialDomain.MAX_COORDINATE, p.getX() + r), Math.min(SpatialDomain.MAX_COORDINATE, p.getY() + r)
            );

            for (String cell : SpatialDomain.getOverlappingCells(nbr, cellSize)) {
                String tag = cell.equals(home) ? "C|" : "S|";
                outKey.set(cell);
                outValue.set(tag + id + "," + p.getX() + "," + p.getY());
                
                // Key: cellX_cellY Value: C|id,x,y (home cell) or S|id,x,y (neighbor cell)
                context.write(outKey, outValue);
            }
        }
    }

    public static class OutlierDetectionReducer
        extends Reducer<Text, Text, Text, Text> {

        private int k;
        // Used for neighbor tests (distance squared) dx²+dy²<=r²
        private long rSquared;
        
         @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("outlier.k", -1);
            int r = context.getConfiguration().getInt("outlier.r", -1);
            if (k < 1 || r < 1) {
                throw new IllegalStateException("outlier.k and outlier.r must be set");
            }
            rSquared = (long) r * r;
        }
        
        @Override
        protected void reduce(Text cellId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            
            List<Long> allIds = new ArrayList<>();
            List<Long> coreIds = new ArrayList<>();
            List<Point> allPoints = new ArrayList<>();
            List<Point> corePoints = new ArrayList<>();
            
            // Aggregate points into allPoints and corePoints lists
            for (Text value : values) {
                String record = value.toString();
                boolean isCore = record.startsWith("C|");
                String[] parts = record.substring(2).split(",");   // id,x,y
                long id = Long.parseLong(parts[0]);
                Point p = new Point(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));

                allIds.add(id);
                allPoints.add(p);
                if (isCore) {
                    coreIds.add(id);
                    corePoints.add(p);
                }
            }
            // Check each core point for outlier status
            for (int i=0;i<corePoints.size();i++) {
                Point p = corePoints.get(i);
                int neighbors = countNeighbors(coreIds.get(i), p, allIds, allPoints);
                if (neighbors < k) {
                    // Key: x,y Value: neighbor count (< k)
                    context.write(new Text(p.toString()), new Text(String.valueOf(neighbors)));
                }
            }
        }

        // Linear Scan: q is a neighbor if squared distance dx²+dy² <= r² (no sqrt needed - cheaper).
        // Skips p's own id; stops at k, so count is exact only for outliers.
        private int countNeighbors(long id, Point p, List<Long> allIds, List<Point> allPoints) {
            int count = 0;
            for (int j = 0; j < allPoints.size(); j++) {
                if (allIds.get(j) == id) {
                    continue;
                }
                // Skip p->count only neighbors within rSquared
                Point q = allPoints.get(j);
                long dx = q.getX() - p.getX();
                long dy = q.getY() - p.getY();
                if (dx * dx + dy * dy <= rSquared && ++count >= k) {
                    break;
                }
            }
            return count;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: OutlierDetection <points dataset> <output directory> <r> <k>");
            System.exit(2);
        }
        int r = Integer.parseInt(args[2]);
        int k = Integer.parseInt(args[3]);
        if (r < 1 || k < 1) {
            throw new IllegalArgumentException("r and k must be >= 1");
        }

        Configuration conf = new Configuration();
        conf.setInt("outlier.r", r);
        conf.setInt("outlier.k", k);
        // Optimization: Ensure cell size is at least default CELL_SIZE or 2r
        conf.setInt("outlier.cellSize", Math.max(SpatialDomain.CELL_SIZE, 2 * r));
        Job job = Job.getInstance(conf, "Outlier Detection");
        job.setJarByClass(OutlierDetection.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(OutlierDetectionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}