package assignment2.backup.chainmapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

// import PDPreProcess.IntSumReducer;
// import PDPreProcess.TokenizerMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.mapreduce.Job;


public class ParallelDijkstra {
    public static class PDMapper extends Mapper<Object, Text, IntWritable, PDNodeWritable> {

        public void map(Object k, Text value, Context context)
                throws IOException, InterruptedException {

            // set all adjacency nodes to reachable if it is reachable
            if (value.isReachable()) {

                MapWritable adjacencyList = value.getList();
                int dis = value.getDistance();

                for (Map.Entry<Writable, Writable> e : adjacencyList.entrySet()) {
                    PDNodeWritable adjacency = new PDNodeWritable();
                    IntWritable adj = (IntWritable) e.getKey();
                    int weight = ((IntWritable) e.getValue()).get();
                    adjacency.updateDistance(key.get(), weight + dis);
                    context.write(adj, adjacency);
                }
            }
        }
    }

    public static class PDReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            boolean reachable = false;
            boolean notYetGotList = true;
            int distance = Integer.MAX_VALUE;
            int prev = 0;
            MapWritable adjacencyList = new MapWritable();

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

                // get the list
                if (notYetGotList && node.hasList()) {
                    adjacencyList = node.getList();
                }
            }

            PDNodeWritable node = new PDNodeWritable(prev, distance, reachable);
            if (!notYetGotList) {
                node.setList(adjacencyList);
            }
            context.write(key, node);
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            boolean reachable = false;
            boolean notYetGotList = true;
            int distance = Integer.MAX_VALUE;
            int prev = 0;
            MapWritable adjacencyList;

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

                // get the list
                if (notYetGotList && node.hasList()) {
                    adjacencyList = node.getList();
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

        Job job = Job.getInstance(conf, "pd");
        ChainMapper.addMapper(job, PDPreProcess.TokenizerMapper.class, Object.class, Text.class, IntWritable.class,
                PDNodeWritable.class, job.getConfiguration());
        ChainReducer.setReducer(job, PDPreProcess.IntSumReducer.class, IntWritable.class, PDNodeWritable.class, IntWritable.class,
                PDNodeWritable.class, job.getConfiguration());

        for (int i = 0; i < itr; i++) {
            ChainMapper.addMapper(job, PDMapper.class, IntWritable.class, PDNodeWritable.class,
                    IntWritable.class, PDNodeWritable.class, job.getConfiguration());
            ChainReducer.setReducer(job, PDReducer.class, IntWritable.class, PDNodeWritable.class,
                    IntWritable.class, PDNodeWritable.class, job.getConfiguration());
        }

        ChainReducer.setReducer(job, FinalReducer.class, IntWritable.class, PDNodeWritable.class,
                IntWritable.class, PDNodeWritable.class, job.getConfiguration());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
