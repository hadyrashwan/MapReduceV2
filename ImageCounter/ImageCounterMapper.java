package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
		  throws IOException, InterruptedException {
	  String[] line = value.toString().split("HTTP/");
	   if(line.length>0) {
	    if(line[0].endsWith(".jpg ")) {  //check whether the text is ending with .jpg
	     context.getCounter("ImageCounter", "jpg").increment(1);
	    } else if(line[0].endsWith(".gif ")) {//check whether the text is ending with .gif
	     context.getCounter("ImageCounter", "gif").increment(1);
	    } else
	     context.getCounter("ImageCounter", "other").increment(1);
	   }
	  }
	 }