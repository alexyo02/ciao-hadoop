package com.dislikecounter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DC_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // Costanti per le colonne d'interesse
    private static final int TITLE_COLUMN = 2;
    private static final int LIKES_COLUMN = 8;
    private static final int DISLIKES_COLUMN = 9;

    private Text keyColumn = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String riga = value.toString();

        // Saltiamo l'intestazione
        if (riga.startsWith("video_id,")) {
            return;
        }

        // Parsing manuale per gestire correttamente le virgole nei titoli
        String[] campi = parseCsvLine(riga);

        // Controllo di sicurezza basato sulla colonna con indice maggiore
        if (campi.length <= DISLIKES_COLUMN) {
            return;
        }

        try {
            String titolo = campi[TITLE_COLUMN];
            long likes = Long.parseLong(campi[LIKES_COLUMN].trim());
            long dislikes = Long.parseLong(campi[DISLIKES_COLUMN].trim());

            if (dislikes > likes) {
                keyColumn.set(titolo);
                // Emettiamo la coppia <titolo, 1>
                context.write(keyColumn, NullWritable.get());
            }

        } catch (NumberFormatException e) {
            // Ignoriamo record con formati numerici errati
        }
    }

    /**
     * Metodo di supporto per splittare i CSV tenendo conto delle virgole racchiuse tra virgolette
     */
    private String[] parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes; // Invertiamo lo stato se incontriamo una virgoletta
            } else if (c == ',' && !inQuotes) {
                // Se troviamo una virgola e non siamo tra virgolette, il campo è chiuso
                result.add(currentField.toString());
                currentField.setLength(0); // Svuotiamo il buffer per il prossimo campo
            } else {
                currentField.append(c); // Aggiungiamo il carattere al campo corrente
            }
        }
        // Aggiungiamo l'ultimo campo
        result.add(currentField.toString());

        return result.toArray(new String[0]);
    }
}