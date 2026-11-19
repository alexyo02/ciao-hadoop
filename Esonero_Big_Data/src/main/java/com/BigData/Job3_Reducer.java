package com.BigData;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job3_Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    // Categoria con la media più alta tra quelle viste finora.
    private String bestCategory = null;
    private double bestAverage = -1;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {

        // long: la somma delle visualizzazioni supera il limite di un int.
        long sum = 0;
        long count = 0;

        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        double average = (double) sum / count;

        // A parità resta la prima in ordine alfabetico, perché le chiavi arrivano ordinate.
        if (average > bestAverage) {
            bestAverage = average;
            bestCategory = key.toString();
        }
    }

    // Chiamato una sola volta, dopo aver visto tutte le categorie.
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (bestCategory != null) {
            // Tronchiamo a due cifre decimali per avere un output leggibile.
            double truncatedAverage = Math.floor(bestAverage * 100) / 100;
            context.write(new Text(bestCategory), new DoubleWritable(truncatedAverage));
        }
    }
}
