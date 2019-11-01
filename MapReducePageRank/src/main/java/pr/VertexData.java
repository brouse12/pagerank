package pr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Custom MapReduce value type.  Stores a vertex's page rank and adjacency list.
 */
public class VertexData implements Writable {
  private double pageRank;
  private List<Integer> adjList;

  // Default constructor
  VertexData() {
    pageRank = 0.0;
    adjList = new LinkedList<>();
  }

  public void set(double pageRank, List<Integer> adjList) {
    this.pageRank = pageRank;
    this.adjList = adjList;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(pageRank);
    for (int edge : adjList) {
      out.writeInt(edge);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.pageRank = in.readDouble();

    this.adjList = new LinkedList<>();

    try {
      while (true) {
        this.adjList.add(in.readInt());
      }
    } catch (EOFException e) {
      // Finished reading in all list data.
    }
  }

  public static VertexData read(DataInput in) throws IOException {
    VertexData v = new VertexData();
    v.readFields(in);
    return v;
  }

  // Parses an input array in the form "vertex ID, page rank, edge 1, edge 2, ..." to new VertexData
  public void parseFromCSV(String[] record) throws IllegalArgumentException {
    if (record.length < 1) {
      throw new IllegalArgumentException("Input record should contain at least a vertex ID and a pagerank value.");
    }

    double pr = Double.parseDouble(record[1]);
    List<Integer> edgeList = new ArrayList<>();
    for (int i = 2; i < record.length; i++) {
      edgeList.add(Integer.parseInt(record[i]));
    }

    set(pr, edgeList);
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder("" + pageRank);
    for (int edge : adjList) {
      output.append(",").append(edge);
    }
    return output.toString();
  }
}



