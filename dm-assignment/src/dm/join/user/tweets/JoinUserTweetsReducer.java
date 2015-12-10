/**The join user tweets reducer class**/
package dm.join.user.tweets;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinUserTweetsReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	
	  Iterator<Text> it1 = values.iterator();
	  Iterator<Text> it2 = values.iterator();
	  boolean userRecordFound = false;
	  boolean tweetRecordFound = false;
	  String userRecord = "";
	  String tweetRecord = "";
	  while (it1.hasNext() && !userRecordFound){
		  String it1Record =   it1.next().toString();
		  userRecord = it1Record.substring(16,it1Record.length());
		  System.out.println("it1Record: " + it1Record);
		  userRecordFound = it1Record.contains("**USER_RECORD**");
		  System.out.println("userRecord: " + userRecordFound);
		  
	  }	 
	  while (it2.hasNext()){
	  	  String it2Record =   it2.next().toString();
			  System.out.println("it2Record: " + it2Record);
			  tweetRecord = it2Record.substring(16,it2Record.length());
			  tweetRecordFound = it2Record.contains("**TWEET_RECORD**");
			  System.out.println("tweetRecordFound: " + tweetRecordFound);
			  
			  if (tweetRecordFound) {
				  String joinedRecord = userRecord.concat(tweetRecord);
				  System.out.println("userRecord: " + userRecord);
				  System.out.println("tweetRecord: " + tweetRecord);
				  System.out.println("joinedRecord: " + joinedRecord);
				  context.write(key, new Text(joinedRecord));  
				  //context.write(new Text(userRecord), new Text(tweetRecord));  
			  }
	  }
   } 
 }
