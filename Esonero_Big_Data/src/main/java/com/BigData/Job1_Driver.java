package com.BigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1_Driver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Uso corretto: Job1_Driver <input_dir> <output_path>");
            System.exit(-1);
        }

        // Memorizziamo l'istante iniziale per misurare
        // il tempo complessivo di esecuzione del job.
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1_DislikeCounter");

        job.setJarByClass(Job1_Driver.class);

        job.setMapperClass(Job1_Mapper.class);
        job.setReducerClass(Job1_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Aggiungiamo come input soltanto i file CSV dei video
        // presenti nella cartella: gli eventuali file JSON delle
        // categorie (usati dal Job 3) vengono ignorati.
        Path inputDir = new Path(args[0]);
        FileSystem fs = inputDir.getFileSystem(conf);
        int csvCount = 0;

        for (FileStatus status : fs.listStatus(inputDir)) {
            if (status.isFile() && status.getPath().getName().endsWith("videos.csv")) {
                FileInputFormat.addInputPath(job, status.getPath());
                csvCount++;
            }
        }

        if (csvCount == 0) {
            System.err.println("Nessun file *videos.csv trovato in " + inputDir);
            System.exit(1);
        }

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
            System.err.println("Job 1 non completato: tempo non valido per il benchmark.");
        }

        // Restituiamo 0 in caso di successo e 1 in caso di errore.
        System.exit(completato ? 0 : 1);
    }
}