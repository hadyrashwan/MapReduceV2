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

public class Testwritables {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, StringPairWritable, LongWritable> mapDriver;
  ReduceDriver<StringPairWritable, LongWritable, Text, LongWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text,  StringPairWritable, LongWritable, Text, LongWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
	  StringPairMapper mapper = new StringPairMapper();
    mapDriver = new MapDriver<LongWritable, Text, StringPairWritable, LongWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<StringPairWritable, LongWritable, Text, LongWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text,  StringPairWritable, LongWritable, Text, LongWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {


	  mapDriver.withInput(new LongWritable(1),new Text("Smith cJoe 1963-08-12 Poughkeepsie, NY"));
	  mapDriver.withOutput( new StringPairWritable("Smith", "cJoe") ,new LongWritable(1));

//	  mapDriver.withInput(new LongWritable(1),new Text("Smith Joe 1963-08-12 Poughkeepsie, NY"));
//	  mapDriver.withOutput( new StringPairWritable("Smith", "Joe"),new LongWritable(1));


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
	  List<LongWritable> in = new ArrayList<LongWritable>();
	  in.add(new LongWritable(1));
	  in.add(new LongWritable(1));
	  
	  reduceDriver.withInput(new StringPairWritable("Smith", "Joe"), in);
	  reduceDriver.withOutput(new Text("(Smith,Joe)"), new LongWritable(2));
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
	    mapReduceDriver.withInput(new LongWritable(1),new Text("Smith Joe 1963-08-12 Poughkeepsie, NY"));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("Smith Joe 1832-01-20 Sacramento, CA"));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("Murphy Alice 2004-06-02 Berlin, MA"));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("Murphy Alice 2004-06-02 Berlin, MA"));
	    mapReduceDriver.withInput(new LongWritable(1),new Text("Murphy Alice 2004-06-02 Berlin, MA"));

	    
	    mapReduceDriver.withOutput(new Text("(Murphy,Alice)"), new LongWritable(3));
	    mapReduceDriver.withOutput(new Text("(Smith,Joe)"), new LongWritable(2));
	    mapReduceDriver.runTest();

    //fail("Please implement test.");

  }
}
