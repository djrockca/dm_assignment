/**The dna mirror reducer class **/
package dm.dna.mirror;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNAMirrorReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  StringBuffer sb = new StringBuffer();
	  Iterator<Text> it = values.iterator();
	  while (it.hasNext()){
	        Text user = (Text) it.next();
	        sb.append(user);
	        sb.append("~");
	  }
	 // String[] users = sb.toString().split("~");
	  //if (users.length >= 2) {
          context.write(key, new Text(sb.toString().replace("~", ",")));  
	  //}
  }
}
