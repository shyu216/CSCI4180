import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;

// import java.util.HashMap;
import java.util.Map;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;

public class PDNodeWritable implements Writable {

    // only for dijkstra part
    private boolean reachable;
    private int prev;
    private int distance;

    private boolean hasL;
    private MapWritable adjacentcyList;

    public PDNodeWritable() {
        super();
        this.prev = 233;
        this.distance = Integer.MAX_VALUE;
        this.reachable = false;
        this.hasL = false;
        this.adjacentcyList = new MapWritable();
    }
    
    public int getPrev() {
        return this.prev;
    }

    public int getDistance() {
        return this.distance;
    }

    public boolean isReachable() {
        return this.reachable;
    }

    public boolean hasList() {
        return this.hasL;
    }

    public void updateDistance(int prev, int dis) {
        if (this.distance > dis) {
            this.distance = dis;
            this.prev = prev;
            this.reachable = true;
        }
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
                + "#" + Integer.toString(distance)
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
        // System.out.println(tail);
        this.prev = Integer.valueOf(input.substring(0, tail)).intValue();
        // System.out.println(this.prev);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        // System.out.println(tail);
        this.distance = Integer.valueOf(input.substring(0, tail)).intValue();
        // System.out.println(this.distance);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        // System.out.println(tail);
        this.reachable = Boolean.valueOf(input.substring(0, tail));
        // System.out.println(this.reachable);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        // System.out.println(tail);
        this.hasL = Boolean.valueOf(input.substring(0, tail));
        // System.out.println(this.hasL);
        input = input.substring(tail + 1);

        tail = input.indexOf('#');
        // System.out.println(tail);
        int k, v;
        while (tail > 0) {
            k = Integer.valueOf(input.substring(0, tail)).intValue();
            input = input.substring(tail + 1);
            // System.out.println(k);

            tail = input.indexOf('#');
            // System.out.println(tail);
            v = Integer.valueOf(input.substring(0, tail)).intValue();
            input = input.substring(tail + 1);
            // System.out.println(v);

            tail = input.indexOf('#');
            // System.out.println(tail);

            addToList(new IntWritable(k), new IntWritable(v));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // super.write(out);

        out.writeInt(this.prev);
        out.writeInt(this.distance);
        out.writeBoolean(this.reachable);
        out.writeBoolean(this.hasL);

        this.adjacentcyList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // super.readFields(in);

        this.prev = in.readInt();
        this.distance = in.readInt();
        this.reachable = in.readBoolean();
        this.hasL = in.readBoolean();

        this.adjacentcyList.readFields(in);
    }
}
