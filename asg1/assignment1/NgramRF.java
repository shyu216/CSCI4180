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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;

import java.util.HashMap;
import java.util.Map;

public class NgramRF {
    public static class MyInputFormat
            extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext job, Path filename) {
            return false;
        }
    }

    // initial string here will work
    public static String ngram = new String("");
    public static int ngramlen = 0;

    public static class TokenizerMapper// only change this function
            extends Mapper<Object, Text, Text, Text> {

        private Text keyout = new Text();
        private Text valout = new Text();

        public static Map<String, Map> stripeMap = new HashMap<>();// use map to store stripes
        public Map<String, Integer> stripe = new HashMap<>();// each value assigned to a stripe

        // imporve performance
        private static int n;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = Integer.parseInt(conf.get("N"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Configuration conf = context.getConfiguration();
            // int n = Integer.parseInt(conf.get("N"));

            String stringvalue = value.toString();
            stringvalue = String.join(" ", stringvalue, "#");// so the string always end with special character

            int startIndex = -1;
            int endIndex = -1;
            int length = stringvalue.length();
            char c;
            String nextword = "";
            String firstword = "";

            //
            // replace none_alphanumeric to space?

            for (int i = 0; i < length; i++) {// read the string char by char, and find the first letter/digit and first
                                              // special character
                c = stringvalue.charAt(i);
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                    if (startIndex == -1) {
                        startIndex = i;// start read when found the first alphanumeric
                    }
                } else {
                    endIndex = i;// end read when found the first non-alphanumeric
                } // read the string char by char, and find the first letter/digit and first
                  // special character
                if (startIndex != -1 && startIndex < endIndex) {
                    nextword = stringvalue.substring(startIndex, endIndex);
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
                        keyout.set(ngram.substring(0, ngram.indexOf(' ')));
                        ngram = ngram.substring(ngram.indexOf(' ') + 1, ngram.length());
                        valout.set(ngram);
                        context.write(keyout, valout);
                        ngramlen -= 1;
                    }
                }
            }
        }

        // public void cleanup(Context context) throws IOException, InterruptedException
        // {
        // // out put together
        // // System.out.println(stripeMap);

        // }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable valout = new DoubleWritable();
        private Text keyout = new Text();

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            Map<String, Integer> subgram = new HashMap<>();
            String tmp;
            int sum = 0;
            double rf = 0.0;

            Configuration conf = context.getConfiguration();
            double theta = Double.parseDouble(conf.get("theta"));// read theta

            for (Text val : values) {
                sum += 1;
                tmp = val.toString();
                if (subgram.containsKey(tmp)) {
                    subgram.put(tmp, subgram.get(tmp) + 1);
                } else {
                    subgram.put(tmp, 1);
                }
            }

            for (String s : subgram.keySet()) {
                rf = 1.0 * subgram.get(s) / sum;
                if (rf >= theta) {
                    keyout.set(String.join(" ", key.toString(), s));
                    valout.set(rf);

                    context.write(keyout, valout);
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("N", args[2]);
        conf.set("theta", args[3]);

        Job job = Job.getInstance(conf, "n-gram rf");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
