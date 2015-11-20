/**The Anagram reducer class groups the values of the sorted keys that came in and 
* checks to see if the values iterator contains more than one word. if the values 
* contain more than one word we have spotted a anagram.
**/
package dm.anagram;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  StringBuffer sb = new StringBuffer();
	  Iterator<Text> it = values.iterator();
	  while (it.hasNext()){
	        Text anagram = (Text) it.next();
	        sb.append(anagram);
	        
	        sb.append("~");
	  }
	  String[] anagrams = sb.toString().split("~");
	  if (anagrams.length >= 2) {
          context.write(key, new Text(sb.toString().replace("~", ",")));  
	  }
  }
}