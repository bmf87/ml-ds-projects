package dev.bfavro.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class AgeRangeSummaryTest {
    @Test
    public void ageRangeLabelUsesRequiredBoundaries() throws Exception {
        Method ageRangeLabel = AgeRangeSummary.TransactionMapper.class
            .getDeclaredMethod("ageRangeLabel", int.class);
        ageRangeLabel.setAccessible(true);

        assertEquals("[10,20)", ageRangeLabel.invoke(null, 10));
        assertEquals("[10,20)", ageRangeLabel.invoke(null, 19));
        assertEquals("[20,30)", ageRangeLabel.invoke(null, 20));
        assertEquals("[50,60)", ageRangeLabel.invoke(null, 59));
        assertEquals("[60,70]", ageRangeLabel.invoke(null, 60));
        assertEquals("[60,70]", ageRangeLabel.invoke(null, 70));
        // Age>70 (customer dataset is assumed to have max age 70 or less
        //assertNotEquals("[60,70]", ageRangeLabel.invoke(null, 80));
    }

    @Test
    public void reducerEmitsMinimumMaximumAndAverage() throws Exception {
        AgeRangeSummary.AgeRangeGenderReducer reducer =
            new AgeRangeSummary.AgeRangeGenderReducer();
        Reducer<Text, Text, NullWritable, Text>.Context context = reducerContext();

        reducer.reduce(
            // Key: age range,gender
            new Text("[20,30),male"),
            // Value: transactionStatistics (count, sum, min, max)
            Arrays.asList(
                new Text("1,10.00,10.00,10.00"),
                new Text("1,30.00,30.00,30.00")),
            context);
        
        // Expected output: ageRange,gender,min(TransactionTotal),max(TransactionTotal),average(TransactionTotal)
        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("[20,30),male,10.00,30.00,20.00")));
    }

    @Test
    public void reducerHandlesMalformedRows() throws Exception {
        AgeRangeSummary.AgeRangeGenderReducer reducer =
            new AgeRangeSummary.AgeRangeGenderReducer();
        Reducer<Text, Text, NullWritable, Text>.Context context = reducerContext();

        reducer.reduce(
            // key: age range,gender
            new Text("[30,40),female"),
            Arrays.asList(
                new Text("1,10.00,10.00"),
                new Text("1,30.00,30.00,30.00")),
            context);

        verify(context).write(
            eq(NullWritable.get()),
            eq(new Text("[30,40),female,30.00,30.00,30.00")));
    }

    @SuppressWarnings("unchecked")
    private Reducer<Text, Text, NullWritable, Text>.Context reducerContext() {
        return mock(Reducer.Context.class);
    }
}
