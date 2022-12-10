import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;

public class PDNodeWritable implements Writable {

    // may be redundant
    // private int nodeId;

    // only for dijkstra part
    private boolean reachable;
    private int prev;
    private int distance;

    private boolean hasL;
    private HashMap<Integer, Integer> adjacentcyList;

    // for mapper in dijkstra
    public PDNodeWritable() {
        super();
        this.prev = 0;
        this.distance = 0;
        this.reachable = false;
        this.hasL = false;
        this.adjacentcyList = new HashMap<>();
    }

    // for reducer in dijkstra
    public PDNodeWritable(int pr, int dis, boolean rea) {
        super();
        this.prev = pr;
        this.distance = dis;
        this.reachable = rea;
        this.hasL = false;
        this.adjacentcyList = new HashMap<>();
    }

    // public int getNodeId() {
    // return this.nodeId;
    // }

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
        if (!reachable) {
            this.prev = prev;
            this.distance = dis;
            this.reachable = true;
        } else if (this.distance > dis) {
            this.distance = dis;
            this.prev = prev;
        }
    }

    public HashMap<Integer, Integer> getList() {
        return this.adjacentcyList;
    }

    public void setList(HashMap<Integer, Integer> lis) {
        this.adjacentcyList = lis;
        this.hasL = true;
    }

    public void addToList(int adj, int dis) {
        this.adjacentcyList.put(adj, dis);
        this.hasL = true;
    }

    // @Override
    public String toString() {
        String output = Integer.toString(prev)
                + "\t" + Integer.toString(distance)
                + "\t" + Boolean.toString(reachable)
                + "\t" + Boolean.toString(hasL);
        for (Map.Entry<Integer, Integer> e : this.adjacentcyList.entrySet()) {
            output = String.join("\t", output,
                    new String(Integer.toString(e.getKey()) + "\t" + Integer.toString(e.getValue())));
        }

        return output + "\t";
    }

    public void fromString(String input) {
        int tail = input.indexOf('\t');
        this.prev = Integer.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        tail = input.indexOf('\t');
        this.distance = Integer.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        tail = input.indexOf('\t');
        this.reachable = Boolean.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        tail = input.indexOf('\t');
        this.hasL = Boolean.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        tail = input.indexOf('\t');
        int k, v;
        while (tail > 0) {
            k = Integer.valueOf(input.substring(0, tail));
            input = input.substring(tail + 1);

            v = Integer.valueOf(input.substring(0, tail));
            input = input.substring(tail + 1);

            addToList(k, v);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // super.write(out);

        out.writeInt(this.prev);
        out.writeInt(this.distance);
        out.writeBoolean(this.reachable);
        out.writeBoolean(this.hasL);

        // write len for easier handle
        out.writeInt(adjacentcyList.size());

        for (Map.Entry<Integer, Integer> e : this.adjacentcyList.entrySet()) {
            out.writeInt(e.getKey());
            out.writeInt(e.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // super.readFields(in);

        this.prev = in.readInt();
        this.distance = in.readInt();
        this.reachable = in.readBoolean();
        this.hasL = in.readBoolean();

        int len = in.readInt();
        while (len > 0) {
            addToList(in.readInt(), in.readInt());
            --len;
        }
    }
}
