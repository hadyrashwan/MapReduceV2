package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  int wordCount = 0;
		
		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {
		  
		  /*
		   * Add the value to the word count counter for this key.
		   */
			wordCount += 1;
		}
		
		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new IntWritable(wordCount));
  }
}
