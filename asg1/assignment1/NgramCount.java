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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.HashMap;
import java.util.Map;

public class NgramCount {
    public static class MyInputFormat extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext job, Path filename) {
            return false;
        }
    }

    // initial string here will work
    public static String ngram = new String("");
    public static int ngramlen = 0;
    public static Map<String, Integer> stripeMap = new HashMap<>();// use map to combine values

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> { // only change this function

        private IntWritable num = new IntWritable();
        private Text word = new Text();

        // imporve performance
        private static int n;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = Integer.parseInt(conf.get("N"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Configuration conf = context.getConfiguration();
            // int n = Integer.parseInt(conf.get("N"));

            String strippedValue = value.toString();
            strippedValue = String.join(" ", strippedValue, "#");// so the string always end with special character

            int startIndex = -1;
            int endIndex = -1;
            int length = strippedValue.length();
            char c;
            String nextword = "";
            String firstword = "";

            // int ngramlen = 0;
            // String ngram = new String("");

            //
            // replace none_alphanumeric to space?
            for (int i = 0; i < length; i++) {// read the string char by char, and find the first letter/digit and first
                                              // special character
                c = strippedValue.charAt(i);
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
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
                    if (ngramlen == 0) {
                        ngram = nextword;
                        ngramlen += 1;
                    } else {
                        ngram = String.join(" ", ngram, nextword);
                        ngramlen += 1;
                    }
                    if (ngramlen == n) {// once meet the length, emit the ngram
                        // firstword = ngram.substring(0,ngram.indexOf(' ') - 1);// the firstword of
                        // Ngram
                        if (stripeMap.containsKey(ngram)) {// check whether the ngram is included
                            stripeMap.put(ngram, stripeMap.get(ngram) + 1);// if yes add the value by 1
                        } else {
                            stripeMap.put(ngram, 1);// if no, generate a new key-value pair
                        }
                        ngram = ngram.substring(ngram.indexOf(' ') + 1, ngram.length());
                        ngramlen -= 1;
                    }
                }
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : stripeMap.keySet()) {
                word.set(key);
                num.set(stripeMap.get(key));
                context.write(word, num);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("N", args[2]);

        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(NgramCount.class);
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