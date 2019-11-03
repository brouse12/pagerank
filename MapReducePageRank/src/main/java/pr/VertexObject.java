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
    boolean isVertexData = false;

    try {
      while (true) {
        edgeList.add(in.readInt());
        isVertexData = true;
      }
    } catch (EOFException e) {
      // Finished reading in all list data.
    }
    if (isVertexData) {
      adjList = edgeList;
    }
  }

  // Parses an input array in the form "vertex ID, page rank, edge 1, edge 2, ..." to new VertexData
  public void parseFromCSV(String[] record) throws IllegalArgumentException {
    if (record == null) {
      throw new IllegalArgumentException("Method must be passed a non-null String.");
    }
    if (record.length < 1) {
      throw new IllegalArgumentException("Input record should contain at least a vertex ID and a pagerank value.");
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
    return output.toString();
  }

}



