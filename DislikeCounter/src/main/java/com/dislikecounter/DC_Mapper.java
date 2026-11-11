package com.dislikecounter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DC_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable uno = new IntWritable(1);
    private Text keyColumn = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convertiamo la riga letta in stringa
        String riga = value.toString();

        // Saltiamo l'intestazione del CSV
        if (riga.startsWith("video_id,")) {
            return;
        }

        // Dividiamo la riga usando la virgola come separatore
        String[] campi = riga.split(",");

        // Verifichiamo che siano presenti le colonne necessarie
        if (campi.length < 10) {
            return;
        }

        try {

            // Estraiamo il titolo
            String titolo = campi[2];

            // Estraiamo likes e dislikes
            long likes = Long.parseLong(campi[8].trim());
            long dislikes = Long.parseLong(campi[9].trim());

            // Controlliamo la condizione richiesta
            if (dislikes > likes) {

                keyColumn.set(titolo);

                // Emettiamo la coppia <titolo, 1>
                context.write(keyColumn, uno);
            }

        } catch (NumberFormatException e) {
            // Ignoriamo i record con valori numerici non validi
        }
    }
}