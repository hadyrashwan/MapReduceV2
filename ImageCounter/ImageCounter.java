package stubs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageCounter extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ImageCounter <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(ImageCounter.class);
    job.setJobName("Image Counter");
    job.setMapperClass(ImageCounterMapper.class); // the mapper is LetterMapper
    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(args[0])); // 1st arg is the source 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // 2nd arg is the destination <will be empty file>
   
    job.setOutputKeyClass(Text.class); // the output key is Text
    job.setOutputValueClass(IntWritable.class); // the output key is special type of integer called IntWritable
    /*
     * TODO implement
     */

    boolean success = job.waitForCompletion(true);
    if (success) {
    	   long jpg = job.getCounters().findCounter("ImageCounter","jpg").getValue();
    	   long gif = job.getCounters().findCounter("ImageCounter","gif").getValue();
    	   long other = job.getCounters().findCounter("ImageCounter","other").getValue();
    	   System.out.println("Jpg=="+jpg);
    	   System.out.println("gif=="+gif);
    	   System.out.println("other=="+other);
      return 0;
    } else
      return 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ImageCounter(), args);
    System.exit(exitCode);
  }
}