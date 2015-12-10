package dm.join.user.tweets;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinUserTweetsUnitTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<Object, Text, Text, Text> mapDriver1;
	MapDriver<Object, Text, Text, Text> mapDriver2;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		JoinUserTweetsMapper1 mapper1 = new JoinUserTweetsMapper1();
		mapDriver1 = new MapDriver<Object, Text, Text, Text>();
		mapDriver1.setMapper(mapper1);

		/*
		 * Set up the mapper test harness.
		 */
		JoinUserTweetsMapper2 mapper2 = new JoinUserTweetsMapper2();
		mapDriver2 = new MapDriver<Object, Text, Text, Text>();
		mapDriver2.setMapper(mapper2);
		
		/*
		 * Set up the reducer test harness.
		 */
		JoinUserTweetsReducer reducer = new JoinUserTweetsReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper1);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper1() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" TODO:
		 * implement
		 */
		mapDriver1.withInput(new Text(), new Text(
		        "Obey_Jony09,Riley Bell,MA"));
		    mapDriver1.withOutput(new Text("Obey_Jony09"), new Text("**USER_RECORD**,Obey_Jony09,Riley Bell,MA"));
		    mapDriver1.runTest();
		    
		//("Please implement test.");

	}


	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper2() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" TODO:
		 * implement
		 */
		mapDriver2.withInput(new Text(), new Text(
		        "396124436476092416,Think about the life you livin but don't think so hard it hurts Life is truly a gift, but at the same it is a curse,Obey_Jony09"));
		    mapDriver2.withOutput(new Text("Obey_Jony09"), new Text("**TWEET_RECORD**,396124436476092416,Think about the life you livin but don't think so hard it hurts Life is truly a gift, but at the same it is a curse,Obey_Jony09"));
		    mapDriver2.runTest();
		    
		//("Please implement test.");

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
		 List<Text> values = new ArrayList<Text>();
		  values.add(new Text("**USER_RECORD**,Obey_Jony09,Riley Bell,MA"));
		 values.add(new Text("**TWEET_RECORD**,396124436476092416,Think about the life you livin but don't think so hard it hurts Life is truly a gift, but at the same it is a curse,Obey_Jony09"));
		  reduceDriver.withInput(new Text("Obey_Jony09"), values);
		  reduceDriver.withOutput(new Text("Obey_Jony09"), new Text("Obey_Jony09,Riley Bell,MA,396124436476092416,Think about the life you livin but don't think so hard it hurts Life is truly a gift, but at the same it is a curse,Obey_Jony09"));
		  //reduceDriver.withOutput(new Text("Obey_Jony09,Riley Bell,MA,"), new Text("396124436476092416,Think about the life you livin but don't think so hard it hurts Life is truly a gift, but at the same it is a curse,Obey_Jony09"));
		   
		  System.out.println("get expeted output" + reduceDriver.getExpectedOutputs());
		  reduceDriver.runTest();

	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {

		mapReduceDriver.addInput(new Pair<Object, Text>("1", new Text(
				"@# !. &the cat act in tic tac toe")));
		
	//	mapReduceDriver.addInput(new Pair<Object, Text>("2", new Text(
		//		"the cat act in tic tac toe")));
		List<Pair<Text, Text>> output = mapReduceDriver.run();
		
		for (Pair<Text, Text> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
	}
}
