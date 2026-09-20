package dev.bfavro.job;

import java.io.IOException;
import java.util.Locale;

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
 * Performs a reduce-side join between customer and transaction CSV datasets.
 * Both mappers emit CustomerID as the intermediate key, causing Hadoop's
 * shuffle to place one customer record and all of its transaction records in
 * the same reducer group.
 *
 * <p>Intermediate values are tagged with {@code C} or {@code T} so the reducer
 * can distinguish customer attributes from transaction measures. The reducer
 * emits one row per customer containing ID, Name, Salary, Transaction count,
 * Transaction-total sum, and Minimum item count as csv text value.</p>
 *
 * <p>This design supports inputs that do not fit in mapper memory, but all
 * customer and transaction records cross the shuffle boundary. It therefore
 * favors generality over the lower network cost of a broadcast join.</p>
 */
public class CustomerJoin {

    /**
     * Reads customer rows and emits {@code CustomerID -> C,Name,Salary}.
     * The {@code C} tag identifies the customer side of the join.
     */
    public static class CustomerMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable customerId = new IntWritable();
        private final Text customerData = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);

            // Reject records that do not match the customer CSV schema.
            if (fields.length != Datasets.CUSTOMER_FIELDS) {
                return;
            }

            customerId.set(Integer.parseInt(fields[0]));

            String name = fields[1];
            String salary = fields[5];

            // metadata tag: "C," prefix denotes customer
            customerData.set("C," + name + "," + salary);
            // return Key: CustomerId, Value: C,name,salary
            context.write(customerId, customerData);
        }
    }

    /**
     * Reads transaction rows and emits
     * {@code CustomerID -> T,TransactionTotal,NumberOfItems}.
     * The {@code T} tag identifies values used for reducer aggregations.
     */
    public static class TransactionMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable customerId = new IntWritable();
        private final Text transactionData = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);

            // Reject records that do not match the transaction CSV schema.
            if (fields.length != Datasets.TRANSACTION_FIELDS) {
                return;
            }

            customerId.set(Integer.parseInt(fields[1]));

            String transactionTotal = fields[2];
            String numberOfItems = fields[3];

            // metadata tag: "T", prefix denotes transaction
            transactionData.set("T," + transactionTotal + "," + numberOfItems);
            // return Key: customerId, Value: T,transactionTotal,numberOfItems
            context.write(customerId, transactionData);
        }
    }

    /**
     * Joins the customer record with every transaction sharing its CustomerID
     * and computes transaction count, total amount, and minimum item count.
     *
     * <p>The output format is
     * {@code CustomerID,Name,Salary,NumberOfTransactions,TotalSum,MinItems}.
     * Transaction-only groups are omitted because they have no matching
     * customer. Customers without transactions are retained with zero-valued
     * aggregates.</p>
     */
    public static class JoinReducer
        extends Reducer<IntWritable, Text, NullWritable, Text> {

        private final Text output = new Text();

        @Override
        public void reduce(IntWritable customerId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            String name = null;
            String salary = null;
            long numberOfTransactions = 0;
            double totalSum = 0.0;
            int minimumItems = Integer.MAX_VALUE;
            
            for (Text value : values) {
                String[] fields = value.toString().split(",", -1);

                // A group can arrive in any order, so identify each record by
                // its mapper-added tag rather than relying on value position.
                if ("C".equals(fields[0])) {
                    name = fields[1];
                    salary = fields[2];
                } else if ("T".equals(fields[0])) {
                    totalSum += Double.parseDouble(fields[1]);
                    numberOfTransactions++;
                    int numberOfItems = Integer.parseInt(fields[2]);
                    minimumItems = Math.min(minimumItems, numberOfItems);
                }
            }
       
            // Enforce an inner join for transaction-only groups. Customer
            // groups with no transactions still proceed to the output.
            if (name == null) {
                return;
            }

            // Integer.MAX_VALUE is only an initialization sentinel and must
            // not appear for a customer that has no transactions.
            int outputMinimum = numberOfTransactions == 0 ? 0 : minimumItems;

            output.set(String.format(
                Locale.US,
                "%d,%s,%s,%d,%.2f,%d",
                customerId.get(),
                name,
                salary,
                numberOfTransactions,
                totalSum,
                outputMinimum));

            context.write(NullWritable.get(), output);
        }   
    }

    /**
     * Configures the two input-specific mappers and the shared join reducer.
     *
     * @param args customer input path, transaction input path, output directory
     * @throws Exception if Hadoop cannot configure or execute the job
     */
    public static void main(String[] args) throws Exception {
        // Expected arguments: customers input, transactions input, output directory.
        if (args.length != 3) {
            System.err.println(
                "Usage: CustomerJoin <customers input> <transactions input> <output directory>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "customer join");
        job.setJarByClass(CustomerJoin.class);

        // MultipleInputs associates each CSV schema with its mapper. Both
        // mappers emit CustomerID, establishing the reducer join key.
        MultipleInputs.addInputPath(
            job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(
            job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        // Shuffle by customerId then send groups to reducer
        job.setReducerClass(JoinReducer.class);

        // Intermediate types differ from the final output key type.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // CustomerID is embedded in the CSV value, so no separate output key
        // is required.
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // <Critical> remove existing output dir before running job
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
