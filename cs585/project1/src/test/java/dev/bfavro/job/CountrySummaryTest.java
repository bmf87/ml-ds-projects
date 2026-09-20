package dev.bfavro.job;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class CountrySummaryTest {
    @Test
    public void reducerCountsCustomersAndFindsTransactionRange() throws Exception {
        CountrySummary.CountryReducer reducer = new CountrySummary.CountryReducer();
        Reducer<IntWritable, Text, NullWritable, Text>.Context context = reducerContext();

        reducer.reduce(
            // Key: country ID
            new IntWritable(4),
            // Value: customerCount=1,hasTransaction,minimumTotal,maximumTotal
            Arrays.asList(
                new Text("1,0,0.00,0.00"),
                new Text("0,1,125.50,125.50"),
                new Text("1,1,11.00,500.00"),
                new Text("0,1,10.01,10.25"),
                new Text("1,1,900.00,1000.00")),
            context);

        // Expected output:  countryCode/ID,customerCount,min(TransactionTotal),max(TransactionTotal)
        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("4,3,10.01,1000.00")));
    }

    @Test
    public void combinerOutputCanBeAggregatedAgainByReducer() throws Exception {
        CountrySummary.CountryCombiner combiner = new CountrySummary.CountryCombiner();
        Reducer<IntWritable, Text, IntWritable, Text>.Context context = combinerContext();

        combiner.reduce(
            new IntWritable(6),
            Arrays.asList(
                new Text("3,1,15.00,700.00"),
                new Text("2,1,10.00,850.00")),
            context);

        verify(context).write(
            eq(new IntWritable(6)),
            eq(new Text("5,1,10.00,850.00")));
    }

    @Test
    public void reducerUsesZeroRangeWhenCountryHasNoTransactions() throws Exception {
        CountrySummary.CountryReducer reducer = new CountrySummary.CountryReducer();
        Reducer<IntWritable, Text, NullWritable, Text>.Context context = reducerContext();

        reducer.reduce(
            new IntWritable(8),
            Collections.singletonList(new Text("3,0,0.00,0.00")),
            context);

        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("8,3,0.00,0.00")));
    }

    @SuppressWarnings("unchecked")
    private Reducer<IntWritable, Text, NullWritable, Text>.Context reducerContext() {
        return mock(Reducer.Context.class);
    }

    @SuppressWarnings("unchecked")
    private Reducer<IntWritable, Text, IntWritable, Text>.Context combinerContext() {
        return mock(Reducer.Context.class);
    }
}