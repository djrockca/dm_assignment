package dm.voting;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VotingMapper2 extends Mapper<Object, Text, Text, LongWritable> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	// System.out.print("value: " + value);
	 String[] voterWorths = value.toString().split("\n");
		 	   String voter = new String();
	   long worth = 0;
	  for(String aVoterWorths:voterWorths)
	  {
		  if (!aVoterWorths.isEmpty()) {
			  //convert the word to lower case
			  aVoterWorths = aVoterWorths.toLowerCase();
			  String[] voterWorth = aVoterWorths.split("[\t]+");
			 // System.out.print(" voter: " + voterWorth[0]);
			  //System.out.print(" worth: " + voterWorth[1]);
			  voter = voterWorth[0];
			  String worthStr = voterWorth[1];
				  worth = Long.parseLong(worthStr);
				  context.write(new Text(voter), new LongWritable(worth));
		  } 
	  }
  }
}
