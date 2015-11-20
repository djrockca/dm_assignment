package dm.anagram;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
	  String[] words = value.toString().split("[ \t]+");
	  
	  String anagram;
	  for(String word:words)
	  {
		  //cleanup to remove special char and digits
		  word = word.replaceAll("[^a-zA-Z]", "");
		
		  //save original anagram
		  anagram = word;
		  
		  //convert the word to lower case
		  word = word.toLowerCase();
		  
		  //convert word to char array and sort
		  char[] array = word.toCharArray();
		  Arrays.sort(array);
		 
		  String sortedWord = new String(array);
		  
		  context.write(new Text(sortedWord), new Text(anagram));
	  }
  }
}
