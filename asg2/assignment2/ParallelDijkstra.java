
import java.io.IOException;
import java.util.Map;
import java.util.ResourceBundle.Control;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ParallelDijkstra {
    public static enum ReachCounter {
        COUNT
    };

    public static class CleanM extends Mapper<Object, Text, IntWritable, PDNodeWritable> {

        public void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {
            String text = v.toString();

            int t = text.indexOf('\t');
            int src = Integer.valueOf(text.substring(0, t)).intValue();

            PDNodeWritable node = new PDNodeWritable();
            node.fromString(text.substring(t + 1));

            context.write(new IntWritable(src), node);
        }
    }

    public static class PDMapper extends Mapper<Object, Text, IntWritable, PDNodeWritable> {

        public void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {

            // string is write by intwritable.tostring and pdnodewritable.tostring
            String text = v.toString();

            int t = text.indexOf('\t');
            int src = Integer.valueOf(text.substring(0, t)).intValue();

            PDNodeWritable node = new PDNodeWritable();
            node.fromString(text.substring(t + 1));

            context.write(new IntWritable(src), node);

            // set all adjacency nodes to reachable if it is reachable
            if (node.isReachable()) {

                MapWritable adjacencyList = node.getList();
                int dis = node.getDistance();

                for (Map.Entry<Writable, Writable> dest : adjacencyList.entrySet()) {

                    PDNodeWritable adjacency = new PDNodeWritable();

                    int weight = ((IntWritable) dest.getValue()).get();

                    adjacency.updateDistance(src, weight + dis);

                    context.write((IntWritable) dest.getKey(), adjacency);
                }
            }
        }
    }

    public static class PDReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            boolean reachable = false;
            int distance = Integer.MAX_VALUE;
            int prev = 0;

            int oldd = 0;
            PDNodeWritable node = new PDNodeWritable();
            for (PDNodeWritable val : values) {

                // if reachable
                if (val.isReachable()) {
                    reachable = true;
                    int newDistance = val.getDistance();

                    // keep first path or last path?!!
                    if (newDistance < distance) {
                        distance = newDistance;
                        prev = val.getPrev();
                    }
                }

                // get the list
                if (val.hasList()) {

                    oldd = val.getDistance();

                    MapWritable adjacencyList = val.getList();
                    for (Map.Entry<Writable, Writable> adj : adjacencyList.entrySet()) {
                        node.addToList((IntWritable) adj.getKey(), (IntWritable) adj.getValue());
                    }
                }
            }

            if (oldd != distance) {
                context.getCounter(ReachCounter.COUNT).increment(1);
            }

            if (reachable) {
                node.updateDistance(prev, distance);
            }

            context.write(key, node);
        }
    }

    public static class CleanR extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {
            for (PDNodeWritable v : values) {
                if (v.isReachable()) {
                    context.write(key, new Text("" + v.getPrev() + "\t" + v.getDistance()));
                        }
                    }
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            boolean reachable = false;
            int distance = Integer.MAX_VALUE;
            int prev = 0;

            for (PDNodeWritable node : values) {

                // if reachable
                if (node.isReachable()) {
                    reachable = true;
                    int newDistance = node.getDistance();

                    // first path or last path?!!
                    if (newDistance < distance) {
                        distance = newDistance;
                        prev = node.getPrev();
                    }
                }
            }

            // print if reachable
            if (reachable) {
                Text result = new Text("" + prev + "\t" + distance);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("src", args[2]);
        int itr = Integer.parseInt(args[3]);
        if (itr < 1) {
            System.out.println("Iteration should be positive");
        }

        String workplace = "md/";

        // preprocess
        Job job = Job.getInstance(conf, "preprocess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PDPreProcess.TokenizerMapper.class);
        job.setReducerClass(PDPreProcess.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(workplace + Integer.toString(0)));

        System.out.println(job.waitForCompletion(true) ? "success" : "fail");

        long counter = 0, c2 = 1;
        for (int i = 1; i < itr; i++) {
            Job nextjob = Job.getInstance(conf, "dijkstra" + Integer.toString(i));
            nextjob.setJarByClass(ParallelDijkstra.class);
            nextjob.setMapperClass(PDMapper.class);
            nextjob.setReducerClass(PDReducer.class);
            nextjob.setOutputKeyClass(IntWritable.class);
            nextjob.setOutputValueClass(PDNodeWritable.class);
            nextjob.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(nextjob, new Path(workplace + Integer.toString(i - 1)));
            FileOutputFormat.setOutputPath(nextjob, new Path(workplace + Integer.toString(i)));

            System.out.println(nextjob.waitForCompletion(true) ? "success" : "fail");

            counter = nextjob.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();

            System.out.println(counter);
            if (counter == 0) {
                itr = i + 1;
                break;
            } else {
                counter = 0;
            }
        }

        // last step
        Job finaljob = Job.getInstance(conf, "dijkstra" + Integer.toString(itr));
        finaljob.setJarByClass(ParallelDijkstra.class);
        finaljob.setMapperClass(CleanM.class);
        finaljob.setReducerClass(CleanR.class);
        finaljob.setOutputKeyClass(IntWritable.class);
        finaljob.setOutputValueClass(PDNodeWritable.class);
        finaljob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(finaljob, new Path(workplace + Integer.toString(itr - 1)));
        FileOutputFormat.setOutputPath(finaljob, new Path(args[1]));

        System.exit(finaljob.waitForCompletion(true) ? 0 : 1);
    }
}
