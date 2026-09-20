package dev.bfavro.job;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dev.bfavro.data.Datasets;

/**
 * Produces customer counts and transaction-total ranges grouped by country code 
 * in one MapReduce job using a broadcast join. Hadoop distributes the relatively
 * small customer file (50k)to each transaction mapper, which builds an in-memory
 * CustomerID-to-CountryCode lookup and avoids a reduce-side join.
 *
 * <p>The transaction mappers initially emit one record for each of the five
 * million transactions. The combiner substantially reduces shuffle traffic by
 * merging those records into partial count, minimum, and maximum statistics
 * for each country (local aggregation) before they reach the reducers. The reducer
 * emits one row per country containing CountryCode, Customer count, Transaction count,
 * Minimum transaction total, and Maximum transaction total as csv text value.</p>
 *
 * <p>This design trades additional mapper memory and lookup initialization for
 * a single shuffle and one MapReduce job. The 50,000-customer
 * lookup fits comfortably in each transaction mapper's memory.</p>
 */
public class CountrySummary {

    private static final String CUSTOMER_CACHE_ALIAS = "customer-country-lkp.csv";

    /**
     * Counts customers by country. Each customer contributes one to the count
     * and no transaction range, represented as {@code 1,0,0.00,0.00}.
     */
     public static class CustomerMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable countryCode = new IntWritable();
        private final Text customerStatistics = new Text("1,0,0.00,0.00");

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);

            // ensure customer dataset
            if (fields.length != Datasets.CUSTOMER_FIELDS) {
                return;
            }

            try {
                countryCode.set(Integer.parseInt(fields[4]));

                // customerCount=1,hasTransaction,minimumTotal,maximumTotal
                // return Key: countryCode, Value: customerStatistics
                context.write(countryCode, customerStatistics);
            } catch (NumberFormatException exception) {
                // Ignore malformed rows or an optional CSV header.
            }
        }
    }

    /**
     * Assigns each transaction to a country using a cached customer lookup.
     * A transaction contributes zero customers and its total as both the local
     * minimum and maximum: {@code 0,1,total,total}.
     */
    public static class TransactionMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final Map<Integer, Integer> customerCountries = new HashMap<>();
        private final IntWritable countryCode = new IntWritable();
        private final Text transactionStatistics = new Text();

        /**
         * Runs once before each transaction mapper processes input records.
         * It loads the localized customer file into a CustomerID-to-CountryCode
         * map so transactions can be grouped by a field not contained in the transaction record.
         */
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
                        int customerCountryCode = Integer.parseInt(fields[4]);

                        customerCountries.put(customerId, customerCountryCode);
                    } catch (NumberFormatException exception) {
                        // Ignore malformed rows or a CSV header
                    }
                }
            }
        }
    
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

                // Lkup country code for the customer
                Integer customerCountryCode =
                    customerCountries.get(customerId);

                if (customerCountryCode == null) {
                    return;
                }

                countryCode.set(customerCountryCode);

                // Recall: transaction contributes zero customers - hence the count is 0
                // customerCount=0,hasTransaction=1,minimumTotal,maximumTotal
                transactionStatistics.set(
                    "0,1," + transactionTotal + "," + transactionTotal);

                // return Key: countryCode, Value: transactionStatistics {0,1,transactionTotal,transactionTotal}
                context.write(countryCode, transactionStatistics);
            } catch (NumberFormatException exception) {
                // Ignore malformed transaction rows
            }
        }

    }

    /**
     * Performs optional local aggregation before mapper output crosses the
     * network. Its output uses the same four-field format as mapper output.
     * Hadoop may run this combiner zero, one, or multiple times safely.
     */
    public static class CountryCombiner
        extends Reducer<IntWritable, Text, IntWritable, Text> {

        private final Text combinedStatistics = new Text();

        @Override
        protected void reduce(IntWritable countryCode, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            combinedStatistics.set(aggregate(values).toIntermediateValue());
            // return Key: countryCode, Value: combinedStatistics
            context.write(countryCode, combinedStatistics);
        }
    }

    /**
     * Produces one final summary row for each country after Hadoop has grouped
     * all mapper and combiner values by CountryCode.
     *
     * <p>The output format is:
     * {@code CountryCode,NumberOfCustomers,MinTransTotal,MaxTransTotal}.</p>
     * Customer count is summed only from customer-mapper contributions. The
     * transaction range is merged from transaction-mapper contributions and
     * any partial ranges emitted by combiners.
     */
    public static class CountryReducer
        extends Reducer<IntWritable, Text, NullWritable, Text> {

        private final Text output = new Text();

        @Override
        protected void reduce(IntWritable countryCode, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            CountryStatistics statistics = aggregate(values);

            // A country with customers but no transactions has no natural
            // minimum or maximum, so the CSV convention here is 0.00,0.00.
            output.set(String.format(
                Locale.US,
                "%d,%d,%.2f,%.2f",
                countryCode.get(),
                statistics.customerCount,
                statistics.hasTransaction ? statistics.minimumTransactionTotal : 0.0,
                statistics.hasTransaction ? statistics.maximumTransactionTotal : 0.0));
            // return Key: NullWritable, Value: output {countryCode, customerCount, minTransactionTotal, maxTransactionTotal}
            context.write(NullWritable.get(), output);
        }
    }

    /**
     * Merges raw mapper contributions or previously combined partial results.
     * Each value has the format
     * {@code customerCount,hasTransaction,minimumTotal,maximumTotal}.
     *
     * @param values statistics associated with one country code
     * @return the merged customer count and transaction range
     */
    private static CountryStatistics aggregate(Iterable<Text> values) {
        CountryStatistics result = new CountryStatistics();

        for (Text value : values) {
            String[] fields = value.toString().split(",", -1);
            if (fields.length != 4) {
                continue;
            }

            try {
                result.customerCount += Long.parseLong(fields[0]);
                // Determine if value represents a transaction
                boolean hasTransaction = "1".equals(fields[1]);
                if (hasTransaction) {
                    result.includeTransactionRange(
                        Double.parseDouble(fields[2]),
                        Double.parseDouble(fields[3]));
                }
            } catch (NumberFormatException exception) {
                // Ignore malformed intermediate values
            }
        }

        return result;
    }

    /**
     * Mutable accumulator for one country's partial aggregate.
     *
     * <p>{@code customerCount} can be added across partitions. Minimum and
     * maximum can also be merged across partitions, making this state suitable
     * for both the combiner and reducer.</p>
     *
     * <p>{@code hasTransaction} distinguishes "no transaction observed" from
     * an actual transaction total. Without it, a placeholder value such as
     * zero could incorrectly become the minimum transaction total.</p>
     */
    private static class CountryStatistics {
        private long customerCount;
        private boolean hasTransaction;
        private double minimumTransactionTotal;
        private double maximumTransactionTotal;

        /**
         * Expands this accumulator's transaction range to include another
         * raw transaction or a partial range produced by a combiner.
         *
         * @param minimum minimum value from the incoming range
         * @param maximum maximum value from the incoming range
         */
        private void includeTransactionRange(double minimum, double maximum) {
            // The first transaction initializes the range. Starting at zero
            // would be wrong because generated transaction totals start at 10.
            if (!hasTransaction) {
                minimumTransactionTotal = minimum;
                maximumTransactionTotal = maximum;
                hasTransaction = true;
                return;
            }

            minimumTransactionTotal = Math.min(minimumTransactionTotal, minimum);
            maximumTransactionTotal = Math.max(maximumTransactionTotal, maximum);
        }

        /**
         * Serializes this accumulator back into the intermediate format used
         * by the mappers and understood by {@link #aggregate(Iterable)}.
         * Important for local aggregation in the combiner!
         * 
         * @return {@code count,hasTransaction,minimum,maximum}
         */
        private String toIntermediateValue() {
            return String.format(
                Locale.US,
                "%d,%d,%.2f,%.2f",
                customerCount,
                hasTransaction ? 1 : 0,
                hasTransaction ? minimumTransactionTotal : 0.0,
                hasTransaction ? maximumTransactionTotal : 0.0);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                "Usage: CountrySummary <customers input> <transactions input> <output directory>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country summary");
        job.setJarByClass(CountrySummary.class);

        // CustomerMapper counts customers by country
        // TransactionMapper uses a cached customer file
        MultipleInputs.addInputPath(
            job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(
            job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        // Localize the customer dataset (args[0])under an alias for every
        // TransactionMapper task; no separate lookup file is created.
        job.addCacheFile(new URI(
            new Path(args[0]).toUri().toString() + "#" + CUSTOMER_CACHE_ALIAS));

        job.setCombinerClass(CountryCombiner.class);
        job.setReducerClass(CountryReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
