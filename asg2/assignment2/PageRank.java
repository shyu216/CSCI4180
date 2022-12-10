import java.io.IOException;
import java.util.Map;
import java.util.ResourceBundle.Control;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
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

    public static class PRMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable> {

        public void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {

            // string is write by intwritable.tostring and pdnodewritable.tostring
            String text = v.toString();

            // get nodeid
            int t = text.indexOf('\t');
            int src = Integer.valueOf(text.substring(0, t)).intValue();

            // read node body
            PRNodeWritable node = new PRNodeWritable();
            node.fromString(text.substring(t + 1));

            // set all adjacency nodes to reachable if it is reachable
            int len = node.getLen();
            if (len > 0) {

                double rank = node.getRank();
                rank = rank / len;
                node.setRank(0);

                String list = node.getList();
                int reader = 0;
                context.write(new IntWritable(src), node);

                for (int i = 0; i < len; i++) {

                    // read dest
                    t = list.indexOf('&');
                    reader = Integer.valueOf(list.substring(0, t));
                    list = list.substring(t + 1);

                    // set rank for dest
                    PRNodeWritable tmp = new PRNodeWritable();
                    tmp.setRank(rank);
                    context.write(new IntWritable(reader), tmp);
                }
            } else {
                context.write(new IntWritable(src), node);
            }
        }
    }

    public static class PRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalPR = 0.0;// total pagerank;

            int readlen, len = 0;
            String list = "";

            for (PRNodeWritable val : values) {

                if (len == 0) {
                    readlen = val.getLen();
                    if (readlen > 0) {
                        len = readlen;
                        list = val.getList();
                    }
                }

                totalPR += val.getRank();// add all PR values

            }

            // System.out.println(totalPR);
            PRNodeWritable node = new PRNodeWritable();
            node.setRank(totalPR);
            node.setList(list, len);
            context.write(key, node);

            // for (PRNodeWritable v : values) {
            // context.write(key, v);
            // }
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

        private static double threshold;
        private static long N;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            threshold = Double.parseDouble(conf.get("threshold"));
            N = Integer.parseInt(conf.get("N"));
        }

        public void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {

            // string is write by intwritable.tostring and pdnodewritable.tostring
            String text = v.toString();

            // get nodeid
            int t = text.indexOf('\t');
            int src = Integer.valueOf(text.substring(0, t)).intValue();

            // read node body
            PRNodeWritable node = new PRNodeWritable();
            node.fromString(text.substring(t + 1));

            // set all adjacency nodes to reachable if it is reachable
            double totalPR = node.getRank();
            if (totalPR > threshold) {
                context.write(new IntWritable(src), new DoubleWritable(totalPR / N));
            }

        }
    }

    public static class SimReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            for (DoubleWritable v : values) {
                context.write(key, v);
            }
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, DoubleWritable> {

        private static double threshold;
        private static long N;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            threshold = Double.parseDouble(conf.get("threshold"));
            N = Integer.parseInt(conf.get("N"));
        }

        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalPR = 0.0;// total pagerank;

            // int readlen, len = 0;
            // String list;

            for (PRNodeWritable val : values) {

                // if (len == 0) {
                // readlen = val.getLen();
                // if (readlen > 0) {
                // len = readlen;
                // list = val.getList();
                // }
                // }

                totalPR += val.getRank();// add all PR values

            }

            if (totalPR > threshold) {
                context.write(key, new DoubleWritable(totalPR / N));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        int itr = Integer.parseInt(args[0]);
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
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(workplace + Integer.toString(0)));

        System.out.println(job.waitForCompletion(true) ? "success" : "fail");

        long counter = job.getCounters().findCounter(PRPreProcess.ReachCounter.COUNT).getValue();

        for (int i = 1; i < itr; i++) {
            Job nextjob = Job.getInstance(conf, "PageRank" + Integer.toString(i));
            nextjob.setJarByClass(PageRank.class);
            nextjob.setMapperClass(PRMapper.class);
            nextjob.setReducerClass(PRReducer.class);
            nextjob.setOutputKeyClass(IntWritable.class);
            nextjob.setOutputValueClass(PRNodeWritable.class);
            nextjob.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(nextjob, new Path(workplace + Integer.toString(i - 1)));
            FileOutputFormat.setOutputPath(nextjob, new Path(workplace + Integer.toString(i)));

            System.out.println(nextjob.waitForCompletion(true) ? "success" : "fail");
        }

        // last step
        conf.set("threshold", args[1]);
        conf.set("N", Long.toString(counter));
        Job finaljob = Job.getInstance(conf, "PageRank" + Integer.toString(itr));
        finaljob.setJarByClass(PageRank.class);
        finaljob.setMapperClass(FinalMapper.class);
        // finaljob.setReducerClass(FinalReducer.class);
        // finaljob.setNumReduceTasks(0);
        finaljob.setReducerClass(SimReducer.class);
        finaljob.setOutputKeyClass(IntWritable.class);
        finaljob.setOutputValueClass(DoubleWritable.class);
        finaljob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(finaljob, new Path(workplace + Integer.toString(itr - 1)));
        FileOutputFormat.setOutputPath(finaljob, new Path(args[3]));

        System.exit(finaljob.waitForCompletion(true) ? 0 : 1);
    }
}