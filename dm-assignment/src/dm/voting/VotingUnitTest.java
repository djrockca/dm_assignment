package dm.voting;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class VotingUnitTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	MapReduceDriver<Object, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver2;
	MapDriver<Object, Text, Text, LongWritable> mapDriver2;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver2;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		VotingMapper1 mapper = new VotingMapper1();
		mapDriver = new MapDriver<Object, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		VotingReducer1 reducer = new VotingReducer1();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
		/*
		 * Set up the mapper test harness.
		 */
		VotingMapper2 mapper2 = new VotingMapper2();
		mapDriver2 = new MapDriver<Object, Text, Text, LongWritable>();
		mapDriver2.setMapper(mapper2);

		/*
		 * Set up the reducer test harness.
		 */
		VotingReducer2 reducer2 = new VotingReducer2();
		reduceDriver2 = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
		reduceDriver2.setReducer(reducer2);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver2 = new MapReduceDriver<Object, Text, Text, LongWritable, Text, LongWritable>();
		mapReduceDriver2.setMapper(mapper2);
		mapReduceDriver2.setReducer(reducer2);
		
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" TODO:
		 * implement
		 */
		
		fail("Please implement test.");

	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		/*
		 * For this test, the reducer's input will be "cat 1 1". The expected
		 * output is "cat 2". TODO: implement
		 */
		fail("Please implement test.");

	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text(
				"A C" + "\n" +
				"A B" + "\n" +
				"B C" + "\n" +
				"C F" + "\n" +
				"F C" + "\n" +
				"A 5"  + "\n" +
				"B 1"  + "\n" +
				"C 11"  + "\n" +
				"D 12"  + "\n" )));
		
		
		List<Pair<Text, Text>> output = mapReduceDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst() + "" + p.getSecond());
			//System.out.println(p.getSecond());
		}
	}
	
	
	/*
	 * Test the mapper and reducer phase working together.
	 */
	@Test
	public void testMapReduce2() throws IOException {

		mapReduceDriver2.addInput(new Pair<Object, Text>("2", new Text(
				"c	5\nb	5\nc	1\nf	11")));
		List<Pair<Text, LongWritable>> output2 = mapReduceDriver2.run();
		
		for (Pair<Text, LongWritable> p1 : output2) {
			System.out.println(p1.getFirst() + " " + p1.getSecond());
		}
		
	}
}
