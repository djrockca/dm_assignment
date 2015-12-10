package dm.join.user.tweets;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinUserTweetsMapper2 extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
  		  String tweetRecord = value.toString();
  	      String splitarray[] = tweetRecord.split(",");
  	      String userId = splitarray[splitarray.length-1].trim();
  	      
  	      context.write(new Text(userId), new Text("**TWEET_RECORD**" + "," + tweetRecord));
  	    }
 }

