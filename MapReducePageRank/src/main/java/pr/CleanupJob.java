package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 *
 */
public class CleanupJob extends Configured implements Tool {
  private static final double JUMP_PROBABILITY = 0.15;
  private static final double LINK_PROBABILITY = 0.85;
  private static final long DOUBLE_TO_LONG_CONVERSION_FACTOR = 1000000000;
  public double totalPageRank; // Used to get total final pageRank for debugging purposes.
  private static final int MINIMUM_MAPPERS = 20;


  public static class CleanupMapper extends Mapper<Object, Text, IntWritable, VertexObject> {
    private final static IntWritable vertexID = new IntWritable();
    private final VertexObject data = new VertexObject();
    private double probJumpToThisPage;
    private double danglingMassAddToEachNode;
    private double pageRankCounter = 0.0;

    @Override
    protected void setup(Mapper.Context context) {
      probJumpToThisPage = context.getConfiguration().getDouble("probJumpToPageN", 0.0);
      danglingMassAddToEachNode = context.getConfiguration().getDouble("danglingMass", 0.0);
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      String[] tokenizedInput = input.toString().split(",");

      // Emit graph structure with updated page ranks, based on last iteration's dangling mass
      vertexID.set(Integer.parseInt(tokenizedInput[0]));
      data.parseFromCSV(tokenizedInput);
      data.setPageRank(probJumpToThisPage + LINK_PROBABILITY * (data.getPageRank() + danglingMassAddToEachNode));
      pageRankCounter += data.getPageRank();
      context.write(vertexID, data);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter pageRank = context.getCounter("Output Counters", "Total Page Rank");
      pageRank.increment((long) (pageRankCounter * DOUBLE_TO_LONG_CONVERSION_FACTOR));
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJobName("Page Rank Cleanup");
    job.setJarByClass(MRPageRank.class);
    job.setMapperClass(CleanupMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VertexObject.class);

    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, new Path(args[1]));

    int vertexCount = Integer.parseInt(args[0]);
    int recordsPerMapper = vertexCount / MINIMUM_MAPPERS;
    job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, recordsPerMapper);

    job.getConfiguration().setDouble("probJumpToPageN", JUMP_PROBABILITY / vertexCount);
    job.getConfiguration().setDouble("danglingMass", Double.parseDouble(args[4]) / vertexCount);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    int outcome = job.waitForCompletion(true) ? 0 : 1;
    Counter totalPageRankMass = job.getCounters().findCounter("Output Counters", "Total Page Rank");
    totalPageRank = (double) totalPageRankMass.getValue() / DOUBLE_TO_LONG_CONVERSION_FACTOR;

    return outcome;
  }
}
