package stubs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgWordLength extends Configured implements Tool {

  public static void main(String[] args) throws Exception {


int exitCode= ToolRunner.run(new Configuration() ,new AvgWordLength() , args  );
System.exit(exitCode);

  }
  
  public int run(String[] args)throws Exception{
  

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
      return -1 ;
      
    }
    

    /*
     * Instantiate a Job object for your job's configuration. 
     */
    Job job = new Job(getConf());
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(AvgWordLength.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("Average Word Length");

    
    /*
     * 
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0])); // 1st arg is the source 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // 2nd arg is the destination 
    
    job.setMapperClass(LetterMapper.class); // the mapper is LetterMapper
    job.setReducerClass(AverageReducer.class); // the reducer is AvergaeReducer
    
    job.setOutputKeyClass(Text.class); // the output key is Text
    job.setOutputValueClass(IntWritable.class); // the output key is special type of intger called IntWritable
    
	  Configuration conf = new Configuration();
	  conf.setBoolean("caseSensitive",true);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
    return success ? 0:1 ;
  }
}

