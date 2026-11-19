package com.BigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Job3_Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final int CATEGORY_COLUMN = 4;
    private static final int VIEWS_COLUMN = 7;

    // Riutilizzati per ogni record, invece di creare nuovi oggetti ogni volta.
    private final Text categoryName = new Text();
    private final LongWritable viewCount = new LongWritable();

    // Dizionario category_id -> nome della categoria, costruito dai JSON.
    private final Map<String, String> categoryNames = new HashMap<>();

    // Eseguito una volta per map task, prima di leggere i record:
    // carica nel dizionario i JSON ricevuti tramite la Distributed Cache.
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles == null) {
            return;
        }

        for (URI uri : cacheFiles) {
            Path path = new Path(uri);
            FileSystem fs = path.getFileSystem(conf);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {

                // Nei JSON ogni "title" segue il proprio "id":
                // basta ricordare l'ultimo id letto e abbinarlo al titolo successivo.
                String line;
                String currentId = null;

                while ((line = reader.readLine()) != null) {
                    line = line.trim();

                    if (line.startsWith("\"id\"")) {
                        currentId = extractValue(line);
                    } else if (line.startsWith("\"title\"") && currentId != null) {
                        categoryNames.put(currentId, extractValue(line));
                        currentId = null;
                    }
                }
            }
        }
    }

    // Estrae il valore da una riga JSON:  "title": "Music",  ->  Music
    private static String extractValue(String line) {
        String value = line.substring(line.indexOf(':') + 1).trim();

        if (value.endsWith(",")) {
            value = value.substring(0, value.length() - 1);
        }

        return value.replace("\"", "").trim();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Intestazione del CSV.
        if (line.startsWith("video_id,")) {
            return;
        }

        String[] fields = CSV_Parser.parseCsvLine(line);

        // Righe che non arrivano alla colonna delle visualizzazioni.
        if (fields.length <= VIEWS_COLUMN) {
            return;
        }

        try {
            int categoryId = Integer.parseInt(fields[CATEGORY_COLUMN].trim());
            long views = Long.parseLong(fields[VIEWS_COLUMN].trim());

            if (categoryId < 0 || views < 0) {
                return;
            }

            String name = categoryNames.get(Integer.toString(categoryId));

            // Id non presente in nessun JSON.
            if (name == null) {
                return;
            }

            categoryName.set(name);
            viewCount.set(views);
            context.write(categoryName, viewCount);
        } catch (NumberFormatException e) {
            // Valori non numerici: il record viene ignorato.
        }
    }
}
