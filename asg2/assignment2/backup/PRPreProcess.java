package assignment2.backup;
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

import assignment2.PRNodeWritable;
import assignment2.PRPreProcess;
import assignment2.PRPreProcess.IntSumReducer;
import assignment2.PRPreProcess.ReachCounter;
import assignment2.PRPreProcess.TokenizerMapper;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PRPreProcess {
    public static enum ReachCounter {
        COUNT
    };
    
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, PRNodeWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             
            StringTokenizer itr = new StringTokenizer(value.toString());
            // int nodeId, des, dis;
            // while (itr.hasMoreTokens()) {
            int nodeId = Integer.valueOf(itr.nextToken());
            int des = Integer.valueOf(itr.nextToken());
            int dis = Integer.valueOf(itr.nextToken());
            PRNodeWritable node = new PRNodeWritable();
            // node.clearList();
            node.addToList(new IntWritable(des), new IntWritable(dis));
            context.write(new IntWritable(nodeId), node);
            // break;
            // }

        }
    }

    public static class IntSumReducer//generate nodes
            extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        private static int src;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            src = Integer.parseInt(conf.get("src"));
        }

        public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                Context context) throws IOException, InterruptedException {

            PRNodeWritable node = new PRNodeWritable();
            context.getCounter(ReachCounter.COUNT).increment(1);//count the amount of nodes
            if (key.get() == src) {
                //context.getCounter(ReachCounter.COUNT).increment(1);
                //node.updateDistance(src, 0);
            }
            for (PRNodeWritable val : values) {
                MapWritable m = val.getList();
                for (Map.Entry<Writable, Writable> e : m.entrySet()) {
                    node.addToList((IntWritable) e.getKey(), (IntWritable) e.getValue());
                }
                // context.write(key, val);
            }
            context.write(key, node);
        }
    }
    
    public static class FinalReducer//initialize page ranks
        extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
         public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                Context context) throws IOException, InterruptedException {
                double PR = 1/context.getCounter(ReachCounter.COUNT);//get the amount of nodes N, set initial pagerank 1/N
                for (PRNodeWritable val : values) {
                    PRNodeWritable node = val;
                    node.updatePagerank(PR);
                    context.write(key, node);
                }
         }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("src", "1");

        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // last step
        Job finaljob = Job.getInstance(conf, "n-gram count" + 1);
         job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(FinalReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}