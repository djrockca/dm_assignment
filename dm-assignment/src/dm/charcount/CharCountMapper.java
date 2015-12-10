/**
 * Mapper receives one line as input.  
 * It cleans up to remove any non alphabets and special characters
 * converts the line to lower case
 * loops through the line characters and emits out put as char, 1 as key, value pair
 */
package dm.charcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  System.out.println("input to mapper: " + line);
	  line = line.replaceAll("[^a-zA-Z]", "");
	 
	  //convert the word to lower case
	  line = line.toLowerCase();
	  
	  System.out.println("cleaned input : " +line);
	  
	  for(int i=0; i < line.length(); i++)
	  {
		  char ch = line.charAt(i);
		  context.write(new IntWritable(ch), new LongWritable(1));
	  }
  }
}
