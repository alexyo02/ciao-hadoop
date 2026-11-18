package com.BigData;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job3_Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    // Migliore categoria finora
    private String bestCategory = null;
    private double bestAverage = -1;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
        long sum = 0;
        long count = 0;

        for (LongWritable v : values) {
            sum += v.get();
            count++;
        }

        double average = (double) sum / count;

        if (average > bestAverage) {
            bestAverage = average;
            bestCategory = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output finale (un solo Reducer)
        if (bestCategory != null) {
            context.write(new Text(bestCategory), new DoubleWritable(bestAverage));
        }
    }
}
