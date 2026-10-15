package dev.bfavro.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dev.bfavro.data.Point;
import dev.bfavro.data.Rectangle;
import dev.bfavro.data.SpatialDomain;
import dev.bfavro.data.Window;

/**
 * Performs a point-in-rectangle spatial join: for each rectangle, report every
 * point whose coordinates lie inside it. An optional spatial window limits the
 * input candidates to points in the window and rectangles that intersect it.
 *
 * The job uses a uniform grid as a heuristic to avoid comparing every point
 * with every rectangle. Each point is assigned to the single grid cell that
 * contains it, while each rectangle is sent to every grid cell it overlaps.
 * The reducer groups records by grid cell and applies a containment test to
 * the candidate pairs. This ensures that a point and any rectangle
 * containing it meet in the same reducer group.
 *
 * Cell size trades off candidate comparisons against shuffle overhead: smaller
 * cells generally reduce the number of candidates per cell but create more
 * keys and rectangle copies. 
 * 
 * Configured cell size can be used as a tuning parameter.
 */
public class SpatialJoin {
    private static final String WINDOW_ENABLED = "spatial.window.enabled";
    private static final String WINDOW_X1 = "spatial.window.x1";
    private static final String WINDOW_Y1 = "spatial.window.y1";
    private static final String WINDOW_X2 = "spatial.window.x2";
    private static final String WINDOW_Y2 = "spatial.window.y2";

    private static Window readWindow(Configuration conf) {
        if (!conf.getBoolean(WINDOW_ENABLED, false)) {
            return null;
        }

        return new Window(
            conf.getInt(WINDOW_X1, 0),
            conf.getInt(WINDOW_Y1, 0),
            conf.getInt(WINDOW_X2, 0),
            conf.getInt(WINDOW_Y2, 0)
        );
    }

    // Parses window arg into Window obj. Ensure fits within the spatial domain
    private static Window parseWindowArgument(String argument) {
        String[] bounds = argument.split(",", -1);
        if (bounds.length != 4) {
            throw new IllegalArgumentException("Window must be x1,y1,x2,y2");
        }

        int x1 = Integer.parseInt(bounds[0].trim());
        int y1 = Integer.parseInt(bounds[1].trim());
        int x2 = Integer.parseInt(bounds[2].trim());
        int y2 = Integer.parseInt(bounds[3].trim());

        if (x1 < SpatialDomain.MIN_COORDINATE || x1 > SpatialDomain.MAX_COORDINATE
                || y1 < SpatialDomain.MIN_COORDINATE || y1 > SpatialDomain.MAX_COORDINATE
                || x2 < SpatialDomain.MIN_COORDINATE || x2 > SpatialDomain.MAX_COORDINATE
                || y2 < SpatialDomain.MIN_COORDINATE || y2 > SpatialDomain.MAX_COORDINATE) {
            throw new IllegalArgumentException("Window coordinates must be within 1..10000");
        }

        return new Window(x1, y1, x2, y2);
    }

    private static boolean isEntirelyOutside(Rectangle r, Window w) {
        return r.getRight() < w.getX1()
            || r.getLeft() > w.getX2()
            || r.getTop() < w.getY1()
            || r.getBottom() > w.getY2();
    }

    private static boolean isEntirelyOutside(Point p, Window w) {
        return p.getX() < w.getX1()
            || p.getX() > w.getX2()
            || p.getY() < w.getY1()
            || p.getY() > w.getY2();
    }

    public static class PointMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private Window window;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            window = readWindow(context.getConfiguration());
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            //String[] fields = value.toString().split("|", -1);
            Point p = SpatialDomain.parsePoint(value.toString());
            
            // (window) provided -> point is outside W, skip
            if (window != null && isEntirelyOutside(p, window)) {
                return;
            }

            String cellId = SpatialDomain.getCellId(p);
            outKey.set(cellId);
            outValue.set("P|" + p.getX() + "," + p.getY());

            // Key: cellX_cellY; Value: P|x,y
            context.write(outKey, outValue);
        }
    }

 
    public static class RectangleMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private Window window;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            window = readWindow(context.getConfiguration());
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            Rectangle r = SpatialDomain.parseRectangle(value.toString());

            // (window) provided -> rectangle is outside W, skip
            if (window != null && isEntirelyOutside(r, window)) {
                return;
            }

            // Find every grid cell the rectangle overlaps
            for (String cellId : SpatialDomain.getOverlappingCells(r)) {
                outKey.set(cellId);
                outValue.set(
                    "R|" +
                    r.id() + "," +
                    r.getBottomLeft().getX() + "," +
                    r.getBottomLeft().getY() + "," +
                    r.getHeight() + "," +
                    r.getWidth()
            );

            // Key: cellX_cellY; Value: R|id,bottomLeft_x,bottomLeft_y,h,w
            context.write(outKey, outValue);
        }
    }
    }

    public static class SpatialJoinReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text cellId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Point> points = new ArrayList<>();
        List<Rectangle> rectangles = new ArrayList<>();

        // Separate P and R
        for (Text value : values) {
            String record = value.toString();
            if (record.startsWith("P|")) {
                // Trim "P|" prefix
                record = record.substring(2); 
                points.add(SpatialDomain.parsePoint(record));
            }
            else if (record.startsWith("R|")) {
                // Trim "R|" prefix
                record = record.substring(2);
                rectangles.add(SpatialDomain.parseRectangle(record));
            }
        }

        // Key: rectangle_id Value: point_x,point_y for matching pairs
        for (Rectangle r : rectangles) {
            for (Point p : points) {
                if (r.contains(p)) {
                    context.write(
                        new Text(r.id()),
                        new Text(p.toString())
                    );
                }
            }
        }
    }
    }

     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Window window = null;
        // Expected arguments: points dataset, rectangles dataset, output directory, optional: window x1,y1,x2,y2
        if (args.length == 4) {
            window = parseWindowArgument(args[3]);
        // Default expected arguments: points dataset, rectangles dataset, output directory
        } else if (args.length != 3) {
            System.err.println(
                "Usage: SpatialJoin <points dataset> <rectangles dataset> <output directory> [x1,y1,x2,y2]");
            System.exit(2);
        }
                  
        if (window != null) {
            conf.setBoolean("spatial.window.enabled", true);
            conf.setInt("spatial.window.x1", window.getX1());
            conf.setInt("spatial.window.y1", window.getY1());
            conf.setInt("spatial.window.x2", window.getX2());
            conf.setInt("spatial.window.y2", window.getY2());
        }
        Job job = Job.getInstance(conf, "Spatial Join");
        job.setJarByClass(SpatialJoin.class);

        // MultipleInputs associates each CSV schema with its mapper. Both
        // mappers emit the cell ID, establishing the reducer join key.
        MultipleInputs.addInputPath(
            job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(
            job, new Path(args[1]), TextInputFormat.class, RectangleMapper.class);

        // Shuffle by cell ID then send groups to reducer
        job.setReducerClass(SpatialJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Docker allocated 12 vCPU
        // Set 4 reduce tasks to balance load
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}