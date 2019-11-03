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
 * Custom MapReduce value type.  Depending on methods called, stores either a vertex's page rank and
 * adjacency list, or an edge's contribution value.  These separate concepts could be more elegantly
 * represented with separate objects implementing the same interface, but MapReduce does not appear
 * to support passing polymorphic objects between mappers and reducers.  This seems to be due to a
 * Reducer optimization where the class in the input iterator is instantiated only once, then
 * repeatedly overwritten via the readFields method.
 */
public class VertexObject implements Writable {
  private double pageRank;
  private List<Integer> adjList;

  VertexObject() {
    pageRank = 0.0;
    adjList = null;
  }

  public boolean isVertexData() {
    return adjList != null;
  }

  public boolean isDanglingPage() {
    if (!isVertexData()) {
      return false;
    }
    return adjList.get(0) == 0;
  }

  public double getPageRank() {
    return pageRank;
  }

  public List<Integer> getAdjList() {
    return adjList;
  }

  public void setPageRank(double pageRank) {
    this.pageRank = pageRank;
  }

  public void setVertexData(double pageRank, List<Integer> adjList) {
    this.pageRank = pageRank;
    this.adjList = adjList;
  }

  public void setContribution(double contribution) {
    this.pageRank = contribution;
  }

  public double getContribution() {
    return pageRank;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(pageRank);
    if (isVertexData()) {
      for (int edge : adjList) {
        out.writeInt(edge);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    adjList = null;
    pageRank = in.readDouble();
    List<Integer> edgeList = new LinkedList<>();
    boolean edgeListExists = false;

    try {
      while (true) {
        edgeList.add(in.readInt());
        edgeListExists = true;
      }
    } catch (EOFException e) {
      // Finished reading in all list data.
    }
    if (edgeListExists) {
      adjList = edgeList;
    }
  }

  // Parses an input array in the form "vertex ID, page rank, edge 1, edge 2, ..." to new VertexData.
  public void parseFromCSV(String[] record) throws IllegalArgumentException {
    if (record == null) {
      throw new IllegalArgumentException("Method must be passed a non-null String.");
    }
    if (record.length < 3) {
      throw new IllegalArgumentException("Input record format: vertexID,rank,edges...");
    }

    double pr = Double.parseDouble(record[1]);
    List<Integer> edgeList = new ArrayList<>();
    for (int i = 2; i < record.length; i++) {
      edgeList.add(Integer.parseInt(record[i]));
    }
    this.pageRank = pr;
    this.adjList = edgeList;
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();
    if (isVertexData()) {
      output.append(pageRank);
      for (int edge : adjList) {
        output.append(",").append(edge);
      }
    }
    // This method outputs only empty strings for edge contributions.
    return output.toString();
  }

}



