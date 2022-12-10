package assignment2.backup;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;

// import java.util.HashMap;
import java.util.Map;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;

public class PRNodeWritable implements Writable {

    // only for dijkstra part
    private boolean reachable;
    private int prev;
    private double pagerank;

    private boolean hasL;
    private MapWritable adjacentcyList;

    public PRNodeWritable() {
        super();
        this.prev = -1;
        this.pagerank = 0;
        this.reachable = false;
        this.hasL = false;
        this.adjacentcyList = new MapWritable();
    }
    
    public int getPrev() {
        return this.prev;
    }

    public double pagerank() {
        return this.pagerank;
    }
    
    void updatePagerank(double newpagerank) {
        this.pagerank = newpagerank;
    }

    public boolean isReachable() {
        return this.reachable;
    }

    public boolean hasList() {
        return this.hasL;
    }

    public MapWritable getList() {
        return this.adjacentcyList;
    }

    public void addToList(IntWritable adj, IntWritable dis) {
        this.adjacentcyList.put(adj, dis);
        this.hasL = true;
    }

    // @Override
    public String toString() {
        String output = Integer.toString(prev)
                + "#" + Integer.toString(pagerank)
                + "#" + Boolean.toString(reachable)
                + "#" + Boolean.toString(hasL);
        for (Map.Entry<Writable, Writable> e : this.adjacentcyList.entrySet()) {
            output = String.join("#", output,
                    ((IntWritable) e.getKey()).toString()
                            + "#" + ((IntWritable) e.getValue()).toString());
        }

        return output + "#";
    }

    public void fromString(String input) {
        int tail = input.indexOf('#');
        System.out.println(tail);
        this.prev = Integer.valueOf(input.substring(0, tail)).intValue();
        System.out.println(this.prev);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        System.out.println(tail);
        this.pagerank = Double.valueOf(input.substring(0, tail)).intValue();
        System.out.println(this.pagerank);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        System.out.println(tail);
        this.reachable = Boolean.valueOf(input.substring(0, tail));
        System.out.println(this.reachable);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        System.out.println(tail);
        this.hasL = Boolean.valueOf(input.substring(0, tail));
        System.out.println(this.hasL);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        System.out.println(tail);
        int k, v;
        while (tail > 0) {
            k = Integer.valueOf(input.substring(0, tail)).intValue();
            input = input.substring(tail + 1);
            System.out.println(k);

            tail = input.indexOf('#');
            System.out.println(tail);
            v = Integer.valueOf(input.substring(0, tail)).intValue();
            input = input.substring(tail + 1);
            System.out.println(v);

            tail = input.indexOf('#');
            System.out.println(tail);

            addToList(new IntWritable(k), new IntWritable(v));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // super.write(out);

        out.writeInt(this.prev);
        out.writeDouble(this.pagerank);
        out.writeBoolean(this.reachable);
        out.writeBoolean(this.hasL);

        this.adjacentcyList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // super.readFields(in);

        this.prev = in.readInt();
        this.pagerank = in.readDouble();
        this.reachable = in.readBoolean();
        this.hasL = in.readBoolean();

        this.adjacentcyList.readFields(in);
    }
}
