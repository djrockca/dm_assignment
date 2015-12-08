/**The Voting reducer 1**/
package dm.voting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VotingReducer1 extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  Iterator<Text> it = values.iterator();
	 
	  String weight = new String();
	  ArrayList<String> votees = new ArrayList<String>();
	  while (it.hasNext()){
		  Text value = (Text) it.next();
		  if  (value.toString().matches("[a-zA-Z]")) {
			  votees.add(value.toString());
	       } else {
	    	   weight = value.toString();
	       }
	  }

	  for (int i=0; i < votees.size(); i++) {
	        String votee = votees.get(i);
	        if (weight.isEmpty()){
	        	
	        }else {
	        	context.write(new Text(votee + "\t"), new Text(weight));  
	        }
	  }
	  
	 
	 
  }
}
