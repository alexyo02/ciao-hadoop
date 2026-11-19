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

    private Text keyColumn = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String riga = value.toString();

        if (riga.startsWith("video_id,")) {
            return;
        }

        // Richiamo il metodo CSV_Parser per evitare problemi con titoli con virgola
        String[] campi = CSV_Parser.parseCsvLine(riga);

        if (campi.length <= DISLIKES_COLUMN) {
            return;
        }

        try {
            String titolo = campi[TITLE_COLUMN];
            long likes = Long.parseLong(campi[LIKES_COLUMN].trim());
            long dislikes = Long.parseLong(campi[DISLIKES_COLUMN].trim());

            if (dislikes > likes) {
                keyColumn.set(titolo);
                context.write(keyColumn, NullWritable.get());
            }

        } catch (NumberFormatException e) {
            // Ignoriamo i record con valori numerici non validi
        }
    }
}