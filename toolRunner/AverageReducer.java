package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	    double characterSum = 0; // sums will be stored here 
	  	double numberOfValues= 0 ; // just a counter 
		for (IntWritable value : values) { // Iterate 
			characterSum += value.get(); 
			numberOfValues++ ;
		}
		double characterAvg = characterSum/numberOfValues ; // get the sum
		context.write(key, new DoubleWritable(characterAvg)); // pass the output to  the outputFormate 

  }
}