package pr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

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
    try {
      while (true) {
        this.adjList.add(in.readInt());
      }
    } catch (EOFException e) {
      // Finished reading adjacency list.
    }
  }

  public static VertexData read(DataInput in) throws IOException {
    VertexData v = new VertexData();
    v.readFields(in);
    return v;
  }

  // Parses an input array in the form "page rank, edge 1, edge 2, ..."
  public static VertexData parseFromCSV(String[] record) throws IllegalArgumentException {
    if (record.length < 1) {
      throw new IllegalArgumentException("Cannot parse record with no pagerank value.");
    }

    double pr = Double.parseDouble(record[0]);
    List<Integer> edgeList = Arrays.stream(record).map(Integer::parseInt).collect(Collectors.toList());
    edgeList.remove(0);

    VertexData v = new VertexData();
    v.set(pr, edgeList);
    return v;
  }
}



