package com.BigData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Costante che identifica la colonna contenente il nome del canale YouTube.
    // Gli indici partono da 0, quindi la quarta colonna ha indice 3.
    private static final int CHANNEL_COLUMN = 3;

    // Oggetto Text utilizzato per memorizzare il nome del canale.
    private Text keyColumn = new Text();

    // Oggetto IntWritable associato a ogni occorrenza del canale.
    // Ogni record valido contribuisce con il valore 1.
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convertiamo il record ricevuto dal Mapper in una stringa Java.
        String riga = value.toString();

        // Saltiamo l'intestazione del file CSV,
        // perché non contiene informazioni su un video.
        if (riga.startsWith("video_id,")) {
            return;
        }

        // Richiamiamo il metodo statico dalla classe esterna CSV_Parser
        String[] campi = CSV_Parser.parseCsvLine(riga);

        // Verifichiamo che la riga contenga almeno le colonne
        // necessarie per accedere al nome del canale.
        if (campi.length <= CHANNEL_COLUMN) {
            return;
        }

        // Estraiamo il nome del canale dalla quarta colonna.
        // trim() elimina eventuali spazi iniziali e finali.
        String canale = campi[CHANNEL_COLUMN].trim();

        // Escludiamo i record con il nome del canale vuoto.
        if (!canale.isEmpty()) {

            // Impostiamo il nome del canale come chiave di output.
            keyColumn.set(canale);

            // Emettiamo la coppia <nomeCanale, 1>.
            // Il Reducer sommerà successivamente tutti gli 1
            // associati allo stesso canale.
            context.write(keyColumn, one);
        }
    }
}