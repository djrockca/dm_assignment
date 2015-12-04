package dm.wordrecommendation;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordRecommendationMapper extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
		 // System.out.print("value: " + value);
		  String[] wordArray = value.toString().split(" ");
		  String[] nextWordArray = null;
		  if (wordArray.length > 0 ) {
			  nextWordArray =  Arrays.copyOfRange(wordArray, 1, wordArray.length);
		  }
		  for(int i=0; i < wordArray.length - 1; i++)
		  {
			  String word = wordArray[i];
			  word = word.replaceAll("[^a-zA-Z]", "").replaceAll("[ \t\n\r\\s+]", "");;
			  word = word.trim().toLowerCase();
			  
			  String nextWord = nextWordArray[i];
			  nextWord = nextWord.replaceAll("[^a-zA-Z]", "").replaceAll("[ \t\r\n\\s+]", "");
			  nextWord = nextWord.trim().toLowerCase(); 
			  
			 // System.out.print("\nword::nextWord = " + word + " ~ " + nextWord);
			  if (nextWord.length() > 0) {
				  context.write(new Text(word), new Text(nextWord));
			  }
		  }
  }
}
