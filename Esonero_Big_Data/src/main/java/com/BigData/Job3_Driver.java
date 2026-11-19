package com.BigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3_Driver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Uso: Job3_Driver <input_dir> <output_dir>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();

        // La modalità di esecuzione dipende dai file XML di configurazione.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job3_MaxAverageViews");
        job.setJarByClass(Job3_Driver.class);

        job.setMapperClass(Job3_Mapper.class);
        job.setReducerClass(Job3_Reducer.class);

        // Il Mapper emette <categoria, views>, il Reducer <categoria, media>.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Un solo Reducer vede tutte le categorie e può trovare il massimo globale.
        job.setNumReduceTasks(1);

        // I CSV diventano l'input del job; i JSON, piccoli, vanno nella
        // Distributed Cache così che ogni Mapper possa leggerli per intero.
        Path inputDir = new Path(args[0]);
        FileSystem fs = inputDir.getFileSystem(conf);
        int csvCount = 0;
        int jsonCount = 0;

        for (FileStatus status : fs.listStatus(inputDir)) {
            String fileName = status.getPath().getName();

            if (fileName.endsWith("videos.csv")) {
                FileInputFormat.addInputPath(job, status.getPath());
                csvCount++;
            } else if (fileName.endsWith("_category_id.json")) {
                job.addCacheFile(status.getPath().toUri());
                jsonCount++;
            }
        }

        if (csvCount == 0 || jsonCount == 0) {
            System.err.println("In " + inputDir + " servono sia i CSV sia i JSON (trovati "
                    + csvCount + " CSV e " + jsonCount + " JSON)");
            System.exit(1);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        long executionTime = System.currentTimeMillis() - startTime;

        // Il tempo vale per il benchmark solo se il job è andato a buon fine.
        if (success) {
            System.out.println("Tempo di esecuzione: " + executionTime + " millisecondi");
        } else {
            System.err.println("Job 3 non completato: tempo non valido per il benchmark.");
        }

        System.exit(success ? 0 : 1);
    }
}
