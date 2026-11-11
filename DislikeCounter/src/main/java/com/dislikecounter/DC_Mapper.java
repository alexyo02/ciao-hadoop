package com.dislikecounter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DC_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable uno = new IntWritable(1);
    private Text keyColumn = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("id")) {
            return;
        }

        // Convertiamo la riga letta in stringa
        String riga = value.toString();

        // Dividiamo la riga usando la virgola come separatore
        String[] campi = riga.split(",");
    }
}
