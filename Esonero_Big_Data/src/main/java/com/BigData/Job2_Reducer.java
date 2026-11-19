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

    private static final int TOP_K = 5;

    // Un canale con il numero delle sue occorrenze.
    private static class ChannelCount {

        final String channel;
        final int count;

        ChannelCount(String channel, int count) {
            this.channel = channel;
            this.count = count;
        }
    }

    // Mette per primo il canale "peggiore": meno occorrenze e,
    // a parità, il nome che viene dopo in ordine alfabetico.
    private final Comparator<ChannelCount> worstFirst =
            Comparator.comparingInt((ChannelCount c) -> c.count)
                    .thenComparing((ChannelCount c) -> c.channel, Comparator.reverseOrder());

    // Contiene al massimo TOP_K canali, con il peggiore sempre in testa.
    private final PriorityQueue<ChannelCount> topChannels = new PriorityQueue<>(worstFirst);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        ChannelCount candidate = new ChannelCount(key.toString(), count);

        if (topChannels.size() < TOP_K) {
            topChannels.offer(candidate);
        } else if (worstFirst.compare(candidate, topChannels.peek()) > 0) {
            // Il candidato batte il peggiore in classifica e ne prende il posto.
            topChannels.poll();
            topChannels.offer(candidate);
        }
    }

    // Chiamato una sola volta, dopo aver visto tutti i canali.
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // La coda non è ordinata: la ordiniamo dal migliore al peggiore.
        List<ChannelCount> ranking = new ArrayList<>(topChannels);
        ranking.sort(worstFirst.reversed());

        for (ChannelCount entry : ranking) {
            context.write(new Text(entry.channel), new IntWritable(entry.count));
        }
    }
}
