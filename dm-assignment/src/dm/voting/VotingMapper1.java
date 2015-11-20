package dm.voting;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.lang.StringUtils;

public class VotingMapper1 extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
	  //System.out.print("value: " + value);
	   String[] voters = value.toString().split("\n");
	   String voteeWorth = new String();
	   String aVoter = new String();
	  for(String voter:voters)
	  {
		  //convert the word to lower case
		  voter = voter.toLowerCase();
		  String[] voterVoteWorth = voter.split("[ \t]+");
		  
		  aVoter = voterVoteWorth[0];
		  voteeWorth = voterVoteWorth[1];
		
		//  System.out.print("aVoter: " + aVoter +  ", voteeWorth :" + voteeWorth + "\n");
		  context.write(new Text(aVoter), new Text(voteeWorth));
	  }
  }
}
