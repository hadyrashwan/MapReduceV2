package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line = value.toString(); // convert the value to string
	    String [] word = line.split(" ",4) ;  // use regular expression to split the line to words
	    if(word.length==4){
//	    String month=word[3].substring(4, 7);
	    	String monthArray [] =word[3].split("/",3);
	    context.write(new Text(word[0]), new Text(monthArray[1])); // pass the values to the shuffle n sort
	    }
	  /* TODO: implement */
	  
  }
}
