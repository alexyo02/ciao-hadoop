package com.dislikecounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DC_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final static IntWritable uno = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        context.write(key, uno);
    }
}