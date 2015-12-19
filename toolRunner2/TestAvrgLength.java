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

public class TestAvrgLength {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
	LetterMapper mapper = new LetterMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    AverageReducer reducer = new AverageReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

	  mapDriver.withInput(new LongWritable(1),new Text("cat  Cato dog"));
	  mapDriver.withOutput(new Text("c"), new IntWritable(3));
	  mapDriver.withOutput(new Text("C"), new IntWritable(4));
	  mapDriver.withOutput(new Text("d"), new IntWritable(3));
	  mapDriver.runTest();
    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     * TODO: implement
     */
	  
    //fail("Please implement test.");

  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {
	  List<IntWritable> in = new ArrayList<IntWritable>();
	  in.add(new IntWritable(3));
	  in.add(new IntWritable(4));
	  
	  reduceDriver.withInput(new Text("c"), in);
	  reduceDriver.withOutput(new Text("c"), new DoubleWritable(3.5));
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
	    mapReduceDriver.withInput(new LongWritable(1),new Text("cat cato dog"));
	    mapReduceDriver.withOutput(new Text("c"), new DoubleWritable(3.5));
	    mapReduceDriver.withOutput(new Text("d"), new DoubleWritable(3));
	    mapReduceDriver.runTest();

    //fail("Please implement test.");

  }
}
