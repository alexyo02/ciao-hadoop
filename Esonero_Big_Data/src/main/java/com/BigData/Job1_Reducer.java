package com.BigData;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job1_Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    // Lo shuffle raggruppa i titoli uguali: scrivendo ogni chiave una sola volta
    // eliminiamo i duplicati dei video rimasti in tendenza per più giorni.
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
