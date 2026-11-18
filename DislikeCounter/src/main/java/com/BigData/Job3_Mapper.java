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
    private final Text categoryKey = new Text();
    private final LongWritable viewsValue = new LongWritable();

    // Dizionario category_id -> nome categoria
    private final Map<String, String> categoryMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        // JSON caricati dal Driver nella Distributed Cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null) {
            return;
        }

        for (URI uri : cacheFiles) {
            Path path = new Path(uri);
            FileSystem fs = path.getFileSystem(conf);

            // Lettura riga per riga: l'ultimo "id" letto si associa al "title" successivo
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {

                String line;
                String currentId = null;

                while ((line = reader.readLine()) != null) {
                    line = line.trim();

                    if (line.startsWith("\"id\"")) {
                        currentId = extractValue(line);           // "id": "10",  -> 10
                    } else if (line.startsWith("\"title\"") && currentId != null) {
                        String categoryName = extractValue(line); // "title": "Music", -> Music
                        categoryMap.put(currentId, categoryName);
                        currentId = null;
                    }
                }
            }
        }
    }

    // "title": "Music",  ->  Music
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

        // Salta l'intestazione
        if (line.startsWith("video_id,")) {
            return;
        }

        // Il parser gestisce le virgole tra virgolette
        String[] fields = CSV_Parser.parseCsvLine(line);

        if (fields.length <= VIEWS_COLUMN) {
            return;
        }

        String categoryString = fields[CATEGORY_COLUMN].trim();
        String viewsString = fields[VIEWS_COLUMN].trim();

        if (categoryString.isEmpty() || viewsString.isEmpty()) {
            return;
        }

        try {

            int categoryId = Integer.parseInt(categoryString);

            long views = Long.parseLong(viewsString);

            if (categoryId < 0 || views < 0) {
                return;
            }

            // ID -> nome categoria
            String categoryName = categoryMap.get(Integer.toString(categoryId));

            // ID assente nei JSON
            if (categoryName == null) {
                return;
            }

            categoryKey.set(categoryName);
            viewsValue.set(views);

            context.write(categoryKey, viewsValue);

        } catch (NumberFormatException e) {
        }
    }
}
