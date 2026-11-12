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

        if (args.length != 2) {
            System.err.println("Uso corretto: Job2_Driver <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Job2_TopChannels");

        job.setJarByClass(Job2_Driver.class);

        job.setMapperClass(Job2_Mapper.class);
        job.setReducerClass(Job2_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Il Reducer emetterà <NomeCanale, Conteggio>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Usiamo un solo Reducer per evitare problemi
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}