/**The word recommendation reducer class **/
package dm.wordrecommendation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordRecommendationReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	  
	 
	  StringBuffer sb = new StringBuffer();
	  Iterator<Text> it = values.iterator();
	
      Map<String, Integer> unsortMap = new HashMap<String, Integer>();
      while (it.hasNext()){
    	 //create map of words as key and frequency count as value
    	  String w = ((Text) it.next()).toString();
    	 //System.out.println("\nw: "+ w);
          Integer n = unsortMap.get(w);
          //System.out.println("\nn: "+ n);
          n = (n == null) ? 1 : ++n;
          unsortMap.put(w, n);
          
      }   
   
       //comparator to sort the list by counts
       Comparator<Map.Entry<String, Integer>> byMapValues = new Comparator<Map.Entry<String, Integer>>() {
           @Override
           public int compare(Map.Entry<String, Integer> left, Map.Entry<String, Integer> right) {
               return left.getValue().compareTo(right.getValue());
           }
       };
       
       // create a list of map entries
       List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>();
       
       // add all items from unsortedMap to list
       list.addAll(unsortMap.entrySet());
       
       // sort the collection
       Collections.sort(list, byMapValues);
       
       //int nextWordCount = 0;
      
       //loop through the list keys to find top 5 next word recommendations
       	for(int i=list.size()-1 ; i >= 0; --i) {
      	     Object nextWord = (Object) list.get(i).getKey();
      	     int topCounterThreshold = list.size()-5;
      	     //append to buffer string nextWord (frequency occurance)
      	     sb.append(nextWord);
      	     sb.append("("+(Object) list.get(i).getValue().toString()+")");
	 	     
      	     if (i==topCounterThreshold) break;
	 	        sb.append(",");
	 	}
      	//System.out.println("total next words for " + key + "  " + nextWordCount);
      
         context.write(key, new Text(sb.toString()));  
	  
  }
}
