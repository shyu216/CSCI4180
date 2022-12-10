
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

            int nodeId = Integer.valueOf(itr.nextToken());
            String de = itr.nextToken();
            // int destId = Integer.valueOf(de);

            PRNodeWritable node = new PRNodeWritable();
            node.setList(de, 1);

            context.write(new IntWritable(nodeId), node);

            // context.write(new IntWritable(destId), new PRNodeWritable());

        }
    }

    public static class IntSumReducer// generate nodes
            extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {

        public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                Context context) throws IOException, InterruptedException {

            context.getCounter(ReachCounter.COUNT).increment(1);

            int sum = 0;
            String list = "&";

            for (PRNodeWritable val : values) {
                String m = val.getList();

                list = String.join("&", m, list);

                sum++;

            }

            PRNodeWritable node = new PRNodeWritable();
            node.setRank(1);
            node.setList(list, sum);

            context.write(key, node);
        }
    }
}