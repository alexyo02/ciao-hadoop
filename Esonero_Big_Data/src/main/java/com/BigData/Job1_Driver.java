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
            System.err.println("Uso: Job1_Driver <input_dir> <output_dir>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();

        // La modalità di esecuzione dipende dai file XML di configurazione.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1_DislikeCounter");
        job.setJarByClass(Job1_Driver.class);

        job.setMapperClass(Job1_Mapper.class);
        job.setReducerClass(Job1_Reducer.class);

        // Ci interessa solo il titolo: il valore delle coppie è vuoto.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Come input usiamo solo i CSV: i JSON nella stessa cartella servono al Job 3.
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

        boolean success = job.waitForCompletion(true);
        long executionTime = System.currentTimeMillis() - startTime;

        // Il tempo vale per il benchmark solo se il job è andato a buon fine.
        if (success) {
            System.out.println("Tempo di esecuzione: " + executionTime + " millisecondi");
        } else {
            System.err.println("Job 1 non completato: tempo non valido per il benchmark.");
        }

        System.exit(success ? 0 : 1);
    }
}
