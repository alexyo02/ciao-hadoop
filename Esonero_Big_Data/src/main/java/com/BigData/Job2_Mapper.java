package com.BigData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int CHANNEL_COLUMN = 3;

    // Ogni record conta come un'occorrenza del suo canale.
    private static final IntWritable ONE = new IntWritable(1);

    // Riutilizzato per ogni record, invece di creare un nuovo oggetto ogni volta.
    private final Text channel = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Intestazione del CSV.
        if (line.startsWith("video_id,")) {
            return;
        }

        String[] fields = CSV_Parser.parseCsvLine(line);

        // Righe che non arrivano alla colonna del canale.
        if (fields.length <= CHANNEL_COLUMN) {
            return;
        }

        String channelName = fields[CHANNEL_COLUMN].trim();

        if (!channelName.isEmpty()) {
            channel.set(channelName);
            context.write(channel, ONE);
        }
    }
}
