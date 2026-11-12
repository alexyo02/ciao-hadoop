package com.BigData;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job2_Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    }