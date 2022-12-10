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

public class PageRank {
    public static enum ReachCounter {
        COUNT
    };

    public static class PRMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable> {

        public void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {

            // string is write by intwritable.tostring and pdnodewritable.tostring
            String text = v.toString();

            int t = text.indexOf('\t');
            int src = Integer.valueOf(text.substring(0, t)).intValue();

            PRNodeWritable node = new PRNodeWritable();
            node.fromString(text.substring(t + 1));

            context.write(new IntWritable(src), node);//send the original node

            // set all adjacency nodes to reachable if it is reachable
            if (node.isReachable()) {

                MapWritable adjacencyList = node.getList();
                int size = adjacencyList.size();//size of adjaceny list
                double PR = node.pagerank();//current nodes's pagerank
                
                for (Map.Entry<Writable, Writable> dest : adjacencyList.entrySet()) {

                    DoubleWritable PRchange = new DoubleWritable();//portion of page rank value from current node
                    PRchange.set(PR/size);//portion of page rank value from current node

                    context.write((IntWritable) dest.getKey(), PR);//emit (key, portion of page rank
                }
            }
        }
    }
    public static class PRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            boolean reachable = false;
            double pr = 0.0;
            PRNodeWritable node = new PRNodeWritable();
            
            for (DoubleWritable val: values) {
                 pr += Double.valueOf(val);//if recieve a double, add it to total page rank
            }
            
            for (PRNodeWritable val : values) {

                // if reachable
                if (val.isReachable()) {
                    reachable = true;
                }

                // get the list
                if (val.hasList()) {
                
                    MapWritable adjacencyList = val.getList();
                    for (Map.Entry<Writable, Writable> adj : adjacencyList.entrySet()) {
                        node.addToList((IntWritable) adj.getKey(), (IntWritable) adj.getValue());
                    }
                }
                
               
                
            }

            if (reachable) {
                node.updatePagerank(pr);
            }

            context.write(key, node);
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            //boolean reachable = false;
            //int distance = Integer.MAX_VALUE;
            double pagerank = 0.0;
            int prev = 0;

            for (PRNodeWritable node : values) {

                 //if reachable
                if (node.isReachable()) {
                    //reachable = true;
                    pagerank = node.pagerank();;
                  }

            // print if reachable
            //if (reachable) {
                Text result = new Text("" + prev + "\t" + String.valueOf(pagerank));
                context.write(key, result);
            //}
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
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.TokenizerMapper.class);
        job.setReducerClass(PRPreProcess.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(workplace + Integer.toString(0)));

        System.out.println(job.waitForCompletion(true) ? "success" : "fail");

        long counter = 0,c2=1;
        for (int i = 1; i < itr; i++) {
            Job nextjob = Job.getInstance(conf, "PageRank" + Integer.toString(i));
            nextjob.setJarByClass(ParallelDijkstra.class);
            nextjob.setMapperClass(PRMapper.class);
            nextjob.setReducerClass(PRReducer.class);
            nextjob.setOutputKeyClass(IntWritable.class);
            nextjob.setOutputValueClass(PRNodeWritable.class);
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
        Job finaljob = Job.getInstance(conf, "PageRank" + Integer.toString(itr));
        finaljob.setJarByClass(PageRank.class);
        finaljob.setMapperClass(PRMapper.class);
        finaljob.setReducerClass(FinalReducer.class);
        finaljob.setOutputKeyClass(IntWritable.class);
        finaljob.setOutputValueClass(PRNodeWritable.class);
        finaljob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(finaljob, new Path(workplace + Integer.toString(itr - 1)));
        FileOutputFormat.setOutputPath(finaljob, new Path(args[1]));

        System.exit(finaljob.waitForCompletion(true) ? 0 : 1);
    }
}