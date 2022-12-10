import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// import java.util.HashMap;
import java.util.Map;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;

public class PRNodeWritable implements Writable {

    // only for dijkstra part
    private double pagerank;

    private int len;
    private Text adjacentcyList;

    public PRNodeWritable() {
        super();
        this.pagerank = 0.0;
        this.len = 0;
        this.adjacentcyList = new Text();
    }
    
    public double getRank() {
        return this.pagerank;
    }

    public void setRank(double rank) {
        this.pagerank = rank;
    }

    public int getLen() {
        return this.len;
    }

    public void setList(String lis, int len) {
        this.adjacentcyList.set(lis);
        this.len = len;
    }

    public String getList() {
        return this.adjacentcyList.toString();
    }

    // @Override
    public String toString() {
        String output = Integer.toString(this.len)
                + "#" + Double.toString(this.pagerank)
                + "#" + this.adjacentcyList.toString();

        return output;
    }

    public void fromString(String input) {
        int tail = input.indexOf('#');
        this.len = Integer.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        this.pagerank = Double.valueOf(input.substring(0, tail));
        
        setList(input.substring(tail + 1),this.len);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // super.write(out);

        out.writeInt(this.len);
        out.writeDouble(this.pagerank);

        this.adjacentcyList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // super.readFields(in);

        this.len = in.readInt();
        this.pagerank = in.readDouble();

        this.adjacentcyList.readFields(in);
    }
}
