package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class MRPageRank extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(MRPageRank.class);
  private static final double JUMP_PROBABILITY = 0.15;
  private static final double LINK_PROBABILITY = 0.85;
  private static final long DOUBLE_TO_LONG_CONVERSION_FACTOR = 1000000000;
  private static final int MINIMUM_MAPPERS = 20;
  public double danglingMass; // Used to pass d-mass from iteration i to (i + 1)

  // Mapper class
  public static class PageRankMapper extends Mapper<Object, Text, IntWritable, VertexObject> {
    private final static IntWritable vertexID = new IntWritable();
    private final VertexObject data = new VertexObject();
    private final IntWritable edgeID = new IntWritable();
    private final VertexObject contribution = new VertexObject();
    private double probJumpToThisPage;
    private double danglingMassAddToEachNode;
    private boolean isFirstIteration;

    @Override
    protected void setup(Mapper.Context context) {
      probJumpToThisPage = context.getConfiguration().getDouble("probJumpToPageN", 0.0);
      danglingMassAddToEachNode = context.getConfiguration().getDouble("danglingMass", 0.0);
      isFirstIteration = context.getConfiguration().getBoolean("isFirstIteration", true);
    }

    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      String[] tokenizedInput = input.toString().split(",");

      // Emit graph structure with updated page ranks, based on last iteration's dangling mass
      vertexID.set(Integer.parseInt(tokenizedInput[0]));
      data.parseFromCSV(tokenizedInput);
      if (!isFirstIteration) {
        data.setPageRank(probJumpToThisPage + LINK_PROBABILITY * (data.getPageRank() + danglingMassAddToEachNode));
      }
      context.write(vertexID, data);

      // Emit contributions to each edge
      contribution.setContribution(data.getPageRank() / data.getAdjList().size());
      for (int link : data.getAdjList()) {
        edgeID.set(link);
        context.write(edgeID, contribution);
      }
    }
  }

  // Reducer class
  public static class PageRankReducer extends Reducer<IntWritable, VertexObject, IntWritable, VertexObject> {
    private VertexObject data = new VertexObject();
    double danglingMassCounter = 0.0;

    @Override
    public void reduce(final IntWritable vertexID, final Iterable<VertexObject> values, final Context context)
            throws IOException, InterruptedException {

      double summedContributions = 0;

      for (final VertexObject value : values) {
        if (value.isVertexData()) {
          data.setVertexData(value.getPageRank(), value.getAdjList());
        } else {
          summedContributions += value.getContribution();
        }
      }
      if (vertexID.get() != 0) {
        data.setPageRank(summedContributions);
        context.write(vertexID, data);
      } else {
        danglingMassCounter += summedContributions;
      }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Counter totalDanglingMass = context.getCounter("Output Counters", "Total Dangling Mass");
      totalDanglingMass.increment((long) (danglingMassCounter * DOUBLE_TO_LONG_CONVERSION_FACTOR));
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJobName("Page Rank");
    job.setJarByClass(MRPageRank.class);
    job.setMapperClass(PageRankMapper.class);
    job.setReducerClass(PageRankReducer.class);
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
    job.getConfiguration().setBoolean("isFirstIteration", args[3].equals("first"));

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    int outcome = job.waitForCompletion(true) ? 0 : 1;

    Counter totalDanglingMass = job.getCounters().findCounter("Output Counters", "Total Dangling Mass");
    danglingMass = (double) totalDanglingMass.getValue() / DOUBLE_TO_LONG_CONVERSION_FACTOR;
    return outcome;
  }

  public static void main(final String[] args) {
    if (args.length != 5) {
      throw new IllegalArgumentException("Usage: <vertexCount> <iterations> <input-dir> <intermediate-dir> <output-dir>");
    }

    String[] currentJobArgs = new String[5];
    currentJobArgs[0] = args[0]; // Vertex count
    currentJobArgs[1] = args[2]; // input directory
    currentJobArgs[2] = args[3] + "/0"; // numbered output directory
    currentJobArgs[3] = "first"; // is this the first iteration?
    currentJobArgs[4] = "0.0"; // dangling node mass from previous iteration

    int numIterations = Integer.parseInt(args[1]);
    double finalDanglingMass = 0.0;
    for (int i = 0; i < numIterations; i++) {

      MRPageRank newIteration = new MRPageRank();
      try {
        ToolRunner.run(newIteration, currentJobArgs);
      } catch (final Exception e) {
        logger.error("", e);
      }

      currentJobArgs[1] = currentJobArgs[2]; // input directory is the previous iteration's output
      currentJobArgs[3] = "not first"; // no longer the first iteration
      currentJobArgs[4] = "" + newIteration.danglingMass;
      currentJobArgs[2] = args[3] + "/" + (i + 1); // increment the output directory

      finalDanglingMass = newIteration.danglingMass;
    }

    currentJobArgs[4] = "" + finalDanglingMass;
    currentJobArgs[2] = args[4];
    CleanupJob finalIteration = new CleanupJob();
    try {
      ToolRunner.run(finalIteration, currentJobArgs);
    } catch (final Exception e) {
      logger.error("", e);
    }
    logger.info("Final total page rank is " + finalIteration.totalPageRank);

  }
}