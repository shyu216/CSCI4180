package assignment2.backup.arraylike;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
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

    private int len;
    private ArrayList<Integer> adjacentcyList;

    // for mapper in dijkstra
    public PDNodeWritable() {
        super();
        this.prev = 0;
        this.distance = 0;
        this.reachable = false;
        this.len = 0;
        this.adjacentcyList = new ArrayList<>();
    }

    // for reducer in dijkstra
    public PDNodeWritable(int pr, int dis, boolean rea) {
        super();
        this.prev = pr;
        this.distance = dis;
        this.reachable = rea;
        this.len = 0;
        this.adjacentcyList = new ArrayList<>();
    }

    public void clearAll() {
        this.prev = 0;
        this.distance = 0;
        this.reachable = false;
        this.len = 0;
        this.adjacentcyList.clear();
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
        return this.len == 0 ? false : true;
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

    public ArrayList<Integer> getList() {
        return this.adjacentcyList;
    }

    public void setList(ArrayList<Integer> lis, int len) {
        this.adjacentcyList = lis;
        this.len = len;
    }

    public void addToList(int adj, int dis) {
        this.adjacentcyList.add(adj);
        this.adjacentcyList.add(dis);
        this.len += 2;
    }

    // @Override
    public String toString() {
        String output = Integer.toString(prev)
                + "\t" + Integer.toString(distance)
                + "\t" + Boolean.toString(reachable)
                + "\t" + Integer.toString(len);
        for (int i = 0; i < len; i += 2) {
            output = String.join("\t", output,
                    Integer.toString(this.adjacentcyList.get(i))
                            + "\t" + Integer.toString(this.adjacentcyList.get(i + 1)));
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
        this.len = Integer.valueOf(input.substring(0, tail));
        input = input.substring(tail + 1);

        for (int i = 0; i < this.len; i += 2) {
            tail = input.indexOf('\t');
            this.adjacentcyList.add(Integer.valueOf(input.substring(0, tail)));
            input = input.substring(tail + 1);
            tail = input.indexOf('\t');
            this.adjacentcyList.add(Integer.valueOf(input.substring(0, tail)));
            input = input.substring(tail + 1);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // super.write(out);

        out.writeInt(this.prev);
        out.writeInt(this.distance);
        out.writeBoolean(this.reachable);
        out.writeInt(this.len);

        for (int i = 0; i < this.len; i += 2) {
            out.writeInt(this.adjacentcyList.get(i));
            out.writeInt(this.adjacentcyList.get(i + 1));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // super.readFields(in);

        this.prev = in.readInt();
        this.distance = in.readInt();
        this.reachable = in.readBoolean();
        this.len = in.readInt();

        for (int i = 0; i < this.len; i += 2) {
            this.adjacentcyList.add(in.readInt());
            this.adjacentcyList.add(in.readInt());
        }
    }
}
