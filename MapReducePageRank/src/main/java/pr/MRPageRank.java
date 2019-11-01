package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text followee = new Text();


    @Override
    public void map(final Object key, final Text input, final Context context)
            throws IOException, InterruptedException {
      String[] tokenizedInput = input.toString().split(",");
      followee.set(tokenizedInput[1]);
      context.write(followee, one);
    }
  }

  // Reducer class
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();


    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      for (final IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJobName("Page Rank");
    job.setJarByClass(MRPageRank.class);
    job.setMapperClass(MRPageRank.TokenizerMapper.class);
    job.setReducerClass(MRPageRank.IntSumReducer.class);
    job.setOutputKeyClass(Text.class); // Fix the output and input types (see chaining!)
    job.setOutputValueClass(IntWritable.class);
//    job.setInputFormatClass();
//    job.setMapOutputKeyClass();
//    job.setMapOutputValueClass();

    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", ",\t");

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