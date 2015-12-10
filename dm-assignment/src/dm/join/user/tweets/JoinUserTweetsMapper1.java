package dm.join.user.tweets;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinUserTweetsMapper1 extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
		  String userRecord = value.toString();
	      String splitarray[] = userRecord.split(",");
	      String userId = splitarray[0].trim();
	      
	      context.write(new Text(userId), new Text("**USER_RECORD**" + "," + userRecord));
	    }
}
