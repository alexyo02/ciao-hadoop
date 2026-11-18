package com.BigData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Job2_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Costante che indica quanti canali devono essere restituiti.
    private static final int TOP_K = 5;

    /**
     * Classe di supporto utilizzata per associare
     * il nome di un canale al numero delle sue occorrenze.
     */
    private static class ChannelCount {

        String channel;
        int count;

        // Costruttore che inizializza il nome del canale e il conteggio.
        ChannelCount(String channel, int count) {
            this.channel = channel;
            this.count = count;
        }
    }

    // Comparatore utilizzato per individuare il peggiore
    // tra i cinque canali attualmente selezionati.
    //
    // Il canale con meno occorrenze ha priorità nella coda.
    // In caso di parità, consideriamo peggiore quello
    // che viene dopo in ordine alfabetico.
    private final Comparator<ChannelCount> worstFirst =
            Comparator.comparingInt((ChannelCount c) -> c.count)
                    .thenComparing(
                            (ChannelCount c) -> c.channel,
                            Comparator.reverseOrder()
                    );

    // Coda di priorità che memorizza al massimo cinque canali.
    // Permette di sostituire il canale peggiore quando
    // ne viene individuato uno con un conteggio maggiore.
    private final PriorityQueue<ChannelCount> top5 =
            new PriorityQueue<>(worstFirst);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // Variabile utilizzata per contare le occorrenze
        // del canale identificato dalla chiave corrente.
        int count = 0;

        // Sommiamo tutti gli 1 emessi dal Mapper
        // per il canale che stiamo elaborando.
        for (IntWritable value : values) {
            count += value.get();
        }

        // Creiamo un oggetto contenente il nome del canale
        // e il numero totale delle sue occorrenze.
        ChannelCount candidate =
                new ChannelCount(key.toString(), count);

        // Se non abbiamo ancora memorizzato cinque canali,
        // aggiungiamo direttamente il candidato.
        if (top5.size() < TOP_K) {

            top5.offer(candidate);

        } else {

            // Confrontiamo il candidato con il peggiore
            // dei cinque canali attualmente selezionati.
            ChannelCount worst = top5.peek();

            // Se il candidato è migliore, sostituiamo
            // il canale con il conteggio più basso.
            if (worstFirst.compare(candidate, worst) > 0) {

                top5.poll();
                top5.offer(candidate);
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // Convertiamo la coda di priorità in una lista,
        // per poter ordinare i risultati finali.
        List<ChannelCount> results = new ArrayList<>(top5);

        // Ordiniamo i canali per conteggio decrescente.
        // In caso di parità, utilizziamo l'ordine alfabetico.
        results.sort(
                Comparator.comparingInt(
                        (ChannelCount c) -> c.count
                ).reversed().thenComparing(c -> c.channel)
        );

        // Scriviamo in output soltanto i cinque canali
        // con il maggior numero di occorrenze.
        for (ChannelCount result : results) {

            context.write(
                    new Text(result.channel),
                    new IntWritable(result.count)
            );
        }
    }
}