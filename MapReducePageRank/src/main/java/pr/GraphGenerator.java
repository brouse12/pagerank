package pr;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Generates a file containing records in the form "vertexID,rank,edges...".  User specifies the
 * initial page rank for each node, the file output directory/name, and graph size k.  The graph
 * will be k chains of k vertices, ensuring dangling pages and pages with no inlinks for testing
 * purposes.
 */
public class GraphGenerator {

  public static void main(final String[] args) {

    if (args.length != 3) {
      throw new IllegalArgumentException("Usage: <k> <page rank> <output directory>");
    }

    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(args[2]), StandardCharsets.UTF_8))) {

      int k = Integer.parseInt(args[0]);
      double initialPageRank = Double.parseDouble(args[1]);
      generateGraph(k, initialPageRank, writer);

    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }

  private static void generateGraph(int k, double pageRank, Writer writer) throws IOException {
    int counter = 1;
    for (int i = 0; i < k; i++) {
      for (int j = 0; j < k - 1; j++) {
        String line = counter + "," + pageRank + "," + (counter + 1) + "\n";
        writer.write(line);
        counter++;
      }
      String line = counter + "," + pageRank + ",0\n";
      writer.write(line);
      counter++;
    }
  }
}
