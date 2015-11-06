package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	boolean myParam =false;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		 myParam = conf.getBoolean("caseSensitive", false);
	}
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	    String line = value.toString(); // convert the value to string
	    for (String word : line.split("\\W+")) { // use regular expression to split the line to words
	    	int wordLength =	word.length() ; // the length of the word
	    	if (wordLength > 0) { //check that nothing else is attached like special characters 
	        String wordCharacter =	word.substring(0, 1); // split on the 1st char for one time 
	        if(myParam)wordCharacter=wordCharacter.toLowerCase(); // I'm not sure how toLowerCase works just in case

	          /*
	           * Call the write method on the Context object to emit a key
	           * and a value from the map method.
	           */
	          context.write(new Text(wordCharacter), new IntWritable(wordLength));
	        }

  }
}}
