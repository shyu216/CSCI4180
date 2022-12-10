package assignment2.backup;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import assignment2.PDNodeWritable;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.HashMap;
import java.util.Map;

public class PDPreprocess {
    public static class MyInputFormat
            extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext job, Path filename) {
            return false;
        }
    }

    // initial string here will work
    public static int[] ngram = new int[3];
    public static int ngramlen = 0;

    public static class TokenizerMapper// only change this function
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable v = new IntWritable();//start vertex
        private Text word = new Text();//(endV,weight) pair

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Configuration conf = context.getConfiguration();
            // int n = Integer.parseInt(conf.get("N"));

            String strippedValue = value.toString();
            strippedValue = String.join(" ", strippedValue, "#");//so the string always end with special character
            
            int startIndex = -1;
            int endIndex = -1;
            int length = strippedValue.length();
            char c;
            String nextword = "";
            String tail = "";
             
            for (int i = 0; i < length; i++) {// read the string char by char, and find the first letter/digit and first special character
                c = strippedValue.charAt(i);
                if (c >= '0' && c <= '9') {
                    if (startIndex == -1) {
                        startIndex = i;// start read when found the first alphanumeric
                    }
                } else {
                    endIndex = i;// end read when found the first non-alphanumeric
                } // read the string char by char, and find the first letter/digit and first
                  // special character
                 if (startIndex != -1 && startIndex < endIndex) {
                    nextword = strippedValue.substring(startIndex, endIndex);
                    // System.out.println("Here");
                    startIndex = -1;// once find a word, reset the startIndex.
                    ngram[ngramlen] = Integer.valueOf(nextword);//put all int into int[3]
                    ngramlen += 1;
                    if (ngramlen == 3) {// once meet the length, emit the ngram
                        v.set(ngram[0]);
                        tail = String.join(",", String.valueOf(ngram[1]),String.valueOf(ngram[2]));
                        word.set(tail);
                        context.write(v, word);
                        ngramlen = 0;
                    }
                }
            }
        
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text, IntWritable, PDNodeWritable> {
        String adj = new String();
        String weight = new String();
        String inputstring = new String();
        int adj_int;
        int weight_int;
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            double RF = 0.0;
            int tail_len = 0;
            PDNodeWritable node = new PDNodeWritable(Integer.valueOf(key));
            for (Text val : values) {//combine the stripes from each mapped. 
                inputstring = val.toString();
                adj = inputstring.substring(0,inputstring.indexOf(",")-1);
                weight = inputstring.substring(inputstring.indexOf(",")+1,inputstring.length());
                adj_int = Integer.valueOf(adj);
                weight_int = Integer.valueOf(weight);
                node.addToList(adj_int,weight_int);
                        
            }//combine the stripes.
            context.write(key, node);
                 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(PDPreprocess.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
