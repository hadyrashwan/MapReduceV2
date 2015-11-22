package stubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class TestPartitioner {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
	 LogMonthMapper mapper = new LogMonthMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    CountReducer reducer = new CountReducer();
    reduceDriver = new ReduceDriver<Text, Text, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

	  mapDriver.withInput(new LongWritable(1),new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "));
	  mapDriver.withOutput(new Text("96.7.4.14"), new Text("Apr"));
	  mapDriver.runTest();
    /*
     * For this test, the mapper's input wilFebl be "1 cat cat dog" 
     * TODO: implement
     */
	  
    //fail("Please implement test.");

  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {
	  List<Text> in = new ArrayList<Text>();
	  in.add(new Text("Feb"));
	  in.add(new Text("Apr"));
	  
	  reduceDriver.withInput(new Text("96.7.4.14"), in);
	  reduceDriver.withOutput(new Text("96.7.4.14"), new IntWritable(2));
	  reduceDriver.runTest();
    /*
     * For this test, the reducer's input will be "cat 1 1".
     * The expected output is "cat 2".
     * TODO: implement
     */
    //fail("Please implement test.");

  }


  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     * The expected output (from the reducer) is "cat 2", "dog 1". 
     * TODO: implement
     */
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.14 - - [24/Feb/2011:04:20:11 -0400] "));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.15 - - [24/Apr/2011:04:20:11 -0400] "));

	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.15 - - [24/Apr/2011:04:20:11 -0400] "));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.15 - - [24/Apr/2011:04:20:11 -0400] "));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("96.7.4.15 - - [24/Apr/2011:04:20:11 -0400] "));

	    mapReduceDriver.withOutput(new Text("96.7.4.14"), new IntWritable(3));
	    mapReduceDriver.withOutput(new Text("96.7.4.15"), new IntWritable(4));

	    mapReduceDriver.runTest();

    //fail("Please implement test.");

  }
}
