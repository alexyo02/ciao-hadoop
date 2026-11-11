package com.dislikecounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DC_Driver {
    public static void main(String[] args) throws Exception {
            if (args.length != 3) {
                System.err.println("Uso corretto: DC_Driver <input_path> <output_path> <indice_colonna>");
                System.exit(-1);
            }
        Configuration conf = new Configuration();
        Job job = new Job.getInstance(conf, "Analisi_CSV");
        job.setJarByClass(DC_Driver.class);
        job.setMapperClass(DC_Mapper.class);
        job.setReducerClass(DC_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
}
}
