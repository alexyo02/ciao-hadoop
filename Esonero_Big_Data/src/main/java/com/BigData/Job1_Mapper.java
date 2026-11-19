package com.BigData;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final int TITLE_COLUMN = 2;
    private static final int LIKES_COLUMN = 8;
    private static final int DISLIKES_COLUMN = 9;

    // Riutilizzato per ogni record, invece di creare un nuovo oggetto ogni volta.
    private final Text title = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Intestazione del CSV.
        if (line.startsWith("video_id,")) {
            return;
        }

        String[] fields = CSV_Parser.parseCsvLine(line);

        // Righe incomplete, come i frammenti delle descrizioni su più righe.
        if (fields.length <= DISLIKES_COLUMN) {
            return;
        }

        try {
            long likes = Long.parseLong(fields[LIKES_COLUMN].trim());
            long dislikes = Long.parseLong(fields[DISLIKES_COLUMN].trim());

            if (dislikes > likes) {
                title.set(fields[TITLE_COLUMN]);
                context.write(title, NullWritable.get());
            }
        } catch (NumberFormatException e) {
            // Valori non numerici: il record viene ignorato.
        }
    }
}
