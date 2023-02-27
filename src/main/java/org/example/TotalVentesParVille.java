package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalVentesParVille {


    // la strucutre de fichier vente.txt

    // date---->ville--> produit----> prix





    public static class VentesMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable vente = new IntWritable(1);
        private Text ville = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(" ");
            ville.set(fields[1]);
            context.write(ville, vente);
        }
    }

    public static class VentesReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable totalVentes = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalVentes.set(sum);
            context.write(key, totalVentes);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total ventes par ville");
        job.setJarByClass(TotalVentesParVille.class);
        job.setMapperClass(VentesMapper.class);
        job.setCombinerClass(VentesReducer.class);
        job.setReducerClass(VentesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
