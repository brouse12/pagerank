package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

  // Mapper class
  public static class PageRankMapper extends Mapper<Object, Text, IntWritable, Writable> {
    private final static IntWritable vertexID = new IntWritable();
    private final VertexData data = new VertexData();


    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      String[] tokenizedInput = input.toString().split(",");

      vertexID.set(Integer.parseInt(tokenizedInput[0]));
      data.parseFromCSV(tokenizedInput);
      context.write(vertexID, data);
    }
  }

  // Reducer class
  public static class PageRankReducer extends Reducer<IntWritable, Writable, IntWritable, VertexData> {

    @Override
    public void reduce(final IntWritable vertexID, final Iterable<Writable> values, final Context context)
            throws IOException, InterruptedException {


      for (final Writable data : values) {
        context.write(vertexID, (VertexData) data);
      }


    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJobName("Page Rank");
    job.setJarByClass(MRPageRank.class);
    job.setMapperClass(PageRankMapper.class);
    job.setReducerClass(PageRankReducer.class);
    job.setOutputKeyClass(IntWritable.class); // Fix the output and input types (see chaining!)
    job.setOutputValueClass(VertexData.class);
//    job.setInputFormatClass();
//    job.setMapOutputKeyClass();
//    job.setMapOutputValueClass();

    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",");

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[3])); // Fix the paths (see chaining!)
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) {
    if (args.length != 4) {
      throw new IllegalArgumentException("Usage: <iterations> <input-dir> <intermediate> <output-dir>");
    }

    try {
      ToolRunner.run(new MRPageRank(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}