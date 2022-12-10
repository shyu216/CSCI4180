package assignment2.backup.chainmapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PDPreProcess {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, PDNodeWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            // int nodeId, des, dis;
            // while (itr.hasMoreTokens()) {
            int nodeId = Integer.valueOf(itr.nextToken());
            int des = Integer.valueOf(itr.nextToken());
            int dis = Integer.valueOf(itr.nextToken());
            PDNodeWritable node = new PDNodeWritable();
            // node.clearList();
            node.addToList(new IntWritable(des), new IntWritable(dis));
            context.write(new IntWritable(nodeId), node);
            // break;
            // }

        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        private static int src;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            src = Integer.parseInt(conf.get("src"));
        }

        public void reduce(IntWritable key, Iterable<PDNodeWritable> values,
                Context context) throws IOException, InterruptedException {

            PDNodeWritable node = new PDNodeWritable();
            if (key.get() == src) {
                node.updateDistance(src, 0);
            }
            for (PDNodeWritable val : values) {
                MapWritable m = val.getList();
                for (Map.Entry<Writable, Writable> e : m.entrySet()) {
                    node.addToList((IntWritable) e.getKey(), (IntWritable) e.getValue());
                }
                // context.write(key, val);
            }
            context.write(key, node);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("src", "1");

        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}