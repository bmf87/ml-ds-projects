package dev.bfavro.job;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class CustomerJoinTest {
    @Test
    public void reducerJoinsCustomerAndAggregatesTransactions() throws Exception {
        CustomerJoin.JoinReducer reducer = new CustomerJoin.JoinReducer();
        Reducer<IntWritable, Text, NullWritable, Text>.Context context = mockContext();

        reducer.reduce(
            // Key: CustomerId
            new IntWritable(7),
             // Value: C,name,salary or T,amount,count
            Arrays.asList(
                new Text("T,25.50,4"),
                new Text("C,Alice,5500.00"),
                new Text("T,10.25,2"),
                new Text("T,100.00,6")),
            context);

        // Expected output: CustomerId,name,salary,count(tx),sum(tx),min(numItems)
        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("7,Alice,5500.00,3,135.75,2")));
    }

    @Test
    public void reducerOutputsZerosWhenCustomerHasNoTransactions() throws Exception {
        CustomerJoin.JoinReducer reducer = new CustomerJoin.JoinReducer();
        Reducer<IntWritable, Text, NullWritable, Text>.Context context = mockContext();

        reducer.reduce(
            new IntWritable(8),
            Collections.singletonList(new Text("C,Bob,4200.00")),
            context);

        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("8,Bob,4200.00,0,0.00,0")));
    }

    @Test
    public void reducerDoesNotOutputTransactionsWithoutCustomer() throws Exception {
        CustomerJoin.JoinReducer reducer = new CustomerJoin.JoinReducer();
        Reducer<IntWritable, Text, NullWritable, Text>.Context context = mockContext();

        reducer.reduce(
            new IntWritable(9),
            Collections.singletonList(new Text("T,75.00,3")),
            context);

        verify(context, never()).write(NullWritable.get(), new Text());
    }

    @SuppressWarnings("unchecked")
    private Reducer<IntWritable, Text, NullWritable, Text>.Context mockContext() {
        return mock(Reducer.Context.class);
    }
}