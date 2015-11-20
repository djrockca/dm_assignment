/**The Anagram reducer class groups the values of the sorted keys that came in and 
* checks to see if the values iterator contains more than one word. if the values 
* contain more than one word we have spotted a anagram.
**/
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
	  
	  StringBuffer sb = new StringBuffer();
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
	        	sb.append(votee + "\t" + weight);
	        	sb.append("\n");
	        }
	  }
	  context.write(key, new Text(sb.toString()));  
	 
	 
  }
}
