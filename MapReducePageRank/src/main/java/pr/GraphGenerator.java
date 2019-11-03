package pr;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class GraphGenerator {

  public static void main(final String[] args) {


    if (args.length != 3) {
      throw new IllegalArgumentException("Usage: <k> <page rank> <output directory>");
    }

    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(args[2]), StandardCharsets.UTF_8))) {

      generateGraph(Integer.parseInt(args[0]), Double.parseDouble(args[1]), writer);

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
