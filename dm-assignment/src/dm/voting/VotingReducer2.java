/**The Voting Reducer 2**/
package dm.voting;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class VotingReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  long sum = 0;
	  for(LongWritable iw:values)
	  {
		  sum += iw.get();
	  }
	  context.write(key, new LongWritable(sum));
	  
  }
}
