package dev.bfavro.job;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dev.bfavro.data.Datasets;


/**
 * Summarizes transaction totals by customer age range and gender in one
 * MapReduce job. The customer CSV is distributed through Hadoop's cache and
 * loaded by each {@link TransactionMapper} during setup as a
 * {@code CustomerID -> AgeRange,Gender} lookup; only transactions are streamed
 * through the mapper input.
 *
 * <p>Each transaction is mapped to an age/gender key and emits a partial
 * {@code count,sum,min,max} value. The reducer merges these values for each of
 * the twelve possible groups and computes the final average as
 * {@code sum / count}. The output format is
 * {@code AgeRange,Gender,MinTransTotal,MaxTransTotal,AvgTransTotal}.</p>
 *
 * <p>This broadcast-join design avoids shuffling customer records and keeps
 * the transaction aggregation scalable, at the cost of one customer lookup
 * per mapper task.</p>
 */
public class AgeRangeSummary {
    
    private static final String CUSTOMER_CACHE_ALIAS = "customer-age-range-lkp.csv";

    
    //  public static class CustomerMapper
    //     extends Mapper<LongWritable, Text, IntWritable, Text> {

    //     private final IntWritable id = new IntWritable();
    //     private final Text customerData = new Text();

    //     @Override
    //     public void map(LongWritable key, Text value, Context context)
    //         throws IOException, InterruptedException {

    //         String[] fields = value.toString().split(",", -1);

    //         // ensure customer dataset
    //         if (fields.length != Datasets.CUSTOMER_FIELDS) {
    //             return;
    //         }

    //         try {
    //             id.set(Integer.parseInt(fields[0]));
    //             // Customer schema: ID, Name, Age, Gender, CountryCode, Salary
    //             // fields[2] = Age, fields[3] = Gender
    //             customerData.set(fields[2] + "," + fields[3]);

    //             // return Key: ID, Value: age,gender
    //             context.write(id, customerData);
    //         } catch (NumberFormatException exception) {
    //             // Ignore malformed rows or an optional CSV header.
    //         }
    //     }

    /**
     * Assigns each transaction to an age-range,gender group using a cached
     * customer lookup. A transaction contributes its total as count, sum,
     * minimum, and maximum: {@code 1,total,total,total}.
     */
    public static class TransactionMapper
        extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<Integer, String> customerGroups = new HashMap<>();
        private final Text groupKey = new Text();
        private final Text transactionStatistics = new Text();

        /** 
         * Returns the required ageRangeLabel for customer age. Note that 
         * max age in the customer dataset (per spec) is assumed to be 70 or less.
         */
        private static String ageRangeLabel(int age) {
            if (age < 20 && age >= 10) return "[10,20)";
            if (age < 30) return "[20,30)";
            if (age < 40) return "[30,40)";
            if (age < 50) return "[40,50)";
            if (age < 60) return "[50,60)";
            if (age < 71) return "[60,70]";
            throw new IllegalArgumentException("Age out of range: " + age);
        }

        // Loads customer demographics from customer cache 
        // needed to classify transactions: age range,gender
        @Override
        protected void setup(Context context) throws IOException {
            try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(CUSTOMER_CACHE_ALIAS),
                StandardCharsets.UTF_8)) {

                String line;

                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",", -1);

                    if (fields.length != Datasets.CUSTOMER_FIELDS) {
                        continue;
                    }

                    try {
                        int customerId = Integer.parseInt(fields[0]);
                        int customerAge = Integer.parseInt(fields[2]);
                        String gender = fields[3];

                        customerGroups.put(customerId, ageRangeLabel(customerAge) + "," + gender);
                    } catch (NumberFormatException exception) {
                        // Ignore malformed rows or a CSV header
                    }
                }
            }
        }
    
        // Emits one transaction aggregate keyed by age range, gender
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);

            if (fields.length != Datasets.TRANSACTION_FIELDS) {
                return;
            }

            try {
                int customerId = Integer.parseInt(fields[1]);
                String transactionTotal = fields[2];

                // Leverage customerGroups map for ageRange,gender
                String group = customerGroups.get(customerId);
                if (group == null) {
                    return;
                }

                groupKey.set(group);

                // count=1,sum,min,max —> sum/min/max all start = transactionTotal
               transactionStatistics.set(String.format(
                    Locale.US,
                    "1,%.2f,%.2f,%.2f",
                    Double.parseDouble(transactionTotal),
                    Double.parseDouble(transactionTotal),
                    Double.parseDouble(transactionTotal)));
                
                // return: Key: groupKey (age range,gender), Value: transactionStatistics (count, sum, min, max)
                context.write(groupKey, transactionStatistics);
            } catch (NumberFormatException exception) {
                // Ignore malformed transaction rows
            }
        }

    }


    /**
     * Merges all transaction aggregates for one age-range,gender group then 
     * emits its minimum, maximum, and average transaction totals.
    */
    public static class AgeRangeGenderReducer
        extends Reducer<Text, Text, NullWritable, Text> {

        private final Text output = new Text();

        protected void reduce(Text groupKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            long count = 0;
            double sum = 0.0;
            double minimum = Double.MAX_VALUE;
            // Start below valid max - 1st value sets maximum
            double maximum = -Double.MAX_VALUE;

            for (Text value : values) {
                String[] fields = value.toString().split(",", -1);
                if (fields.length != 4) {
                    continue;
                }

                try {
                    count += Long.parseLong(fields[0]);
                    sum += Double.parseDouble(fields[1]);
                    minimum = Math.min(minimum, Double.parseDouble(fields[2]));
                    maximum = Math.max(maximum, Double.parseDouble(fields[3]));

                 
                } catch (NumberFormatException exception) {
                    // Ignore malformed rows
                }
            }

            double average = sum / count;

            output.set(String.format(
                Locale.US,
                "%s,%.2f,%.2f,%.2f",
                groupKey.toString(),
                minimum,
                maximum,
                average));
            
            // return: Key: NullWritable, Value: output (groupKey: age range and gender, minimum, maximum, average)
            context.write(NullWritable.get(), output);
        }
    }

            
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                "Usage: AgeRangeSummary <customers input> <transactions input> <output directory>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "age range summary");
        job.setJarByClass(AgeRangeSummary.class);

        // Transactions are the only streamed input. The customer file is
        // distributed as a lookup for TransactionMapper.setup()
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.addCacheFile(new URI(
            new Path(args[0]).toUri().toString() + "#" + CUSTOMER_CACHE_ALIAS));

        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(AgeRangeGenderReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
