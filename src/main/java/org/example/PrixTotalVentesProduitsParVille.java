package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrixTotalVentesProduitsParVille {

    public static class VentesMapper
            extends Mapper<LongWritable, Text, Text, FloatWritable>{

        private Text villeProduit = new Text();
        private FloatWritable prix = new FloatWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(" ");
            String ville = fields[1];
            String produit = fields[2];
            float prixUnitaire = Float.parseFloat(fields[3]);
            int annee = Integer.parseInt(fields[0].substring(0, 4));
            if(annee == 2021) { // changer l'année selon les besoins
                villeProduit.set(ville + " " + produit);
                prix.set(prixUnitaire);
                context.write(villeProduit, prix);
            }
        }
    }

    public static class VentesReducer
            extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private FloatWritable totalPrix = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            totalPrix.set(sum);
            context.write(key, totalPrix);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "prix total ventes produits par ville");
        job.setJarByClass(PrixTotalVentesProduitsParVille.class);
        job.setMapperClass(VentesMapper.class);
        job.setCombinerClass(VentesReducer.class);
        job.setReducerClass(VentesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
