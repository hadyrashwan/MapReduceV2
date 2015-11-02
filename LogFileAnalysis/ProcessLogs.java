package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessLogs {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProcessLogs.class);
    job.setJobName("Process Logs");

    FileInputFormat.setInputPaths(job, new Path(args[0])); // 1st arg is the source 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // 2nd arg is the destination 
    
    job.setMapperClass(LogFileMapper.class); // the mapper is LetterMapper
    job.setReducerClass(SumReducer.class); // the reducer is AvergaeReducer
    
    job.setOutputKeyClass(Text.class); // the output key is Text
    job.setOutputValueClass(IntWritable.class); // the output key is special type of intger called IntWritable
    
    /*
     * TODO implement
     */

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
