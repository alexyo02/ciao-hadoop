package com.BigData;

import java.util.ArrayList;
import java.util.List;

// Divide una riga CSV nei suoi campi. Le virgole racchiuse tra virgolette
// (ad esempio nei titoli) fanno parte del campo; le virgolette vengono rimosse.
public class CSV_Parser {

    public static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(c);
            }
        }
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }
}
