package com.BigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2_Driver {

    public static void main(String[] args) throws Exception {

        // Verifichiamo che siano stati specificati
        // i percorsi di input e output del job.
        if (args.length != 2) {
            System.err.println("Uso corretto: Job2_Driver <input_path> <output_path>");
            System.exit(-1);
        }

        // Memorizziamo l'istante iniziale per misurare
        // il tempo complessivo di esecuzione del job.
        long startTime = System.currentTimeMillis();

        // Creiamo la configurazione Hadoop.
        // La modalità di esecuzione dipenderà dai file XML.
        Configuration conf = new Configuration();

        // Creiamo il job MapReduce e gli assegniamo un nome.
        Job job = Job.getInstance(conf, "Job2_TopChannels");

        // Indichiamo la classe principale contenuta nel JAR.
        job.setJarByClass(Job2_Driver.class);

        // Associamo al job il Mapper e il Reducer.
        job.setMapperClass(Job2_Mapper.class);
        job.setReducerClass(Job2_Reducer.class);

        // Il Mapper emette coppie <NomeCanale, 1>.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Il Reducer restituisce coppie
        // <NomeCanale, NumeroOccorrenze>.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Utilizziamo un solo Reducer per ottenere
        // i cinque canali migliori sull'intero dataset.
        job.setNumReduceTasks(1);

        // Specifichiamo i percorsi dei dati di input e output.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Avviamo il job e attendiamo che termini.
        boolean completato = job.waitForCompletion(true);

        // Memorizziamo l'istante finale.
        long endTime = System.currentTimeMillis();

        // Calcoliamo la durata in millisecondi.
        long executionTime = endTime - startTime;

        // Stampiamo il tempo soltanto se il job è riuscito.
        if (completato) {
            System.out.println("Tempo di esecuzione: "
                    + executionTime + " millisecondi");
        } else {
            System.err.println("Job 2 non completato: tempo non valido per il benchmark.");
        }

        // Restituiamo 0 in caso di successo e 1 in caso di errore.
        System.exit(completato ? 0 : 1);
    }
}