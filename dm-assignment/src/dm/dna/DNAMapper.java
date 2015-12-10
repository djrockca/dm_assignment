/** Mapper for processing dna file
 * Split each line 
 * loop through array of lines where each word is delimited by tab
 * find key value pair  where key is user and value is dna
 * emit key value pair
 */
package dm.dna;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DNAMapper extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
	 // System.out.print("value: " + value);
	  String[] dnaUsers = value.toString().split("\n");
	  String dna = new String();
	  String user = new String();
	  for(String dnaUser:dnaUsers)
	  {
		  //convert the word to lower case
		  dnaUser = dnaUser.toLowerCase();
		 // System.out.print("dnaUser: " + dnaUser + "\n");
		  String[] dnaUserArray= dnaUser.split("[ \t]+");
		  
		  user = dnaUserArray[0];
		  dna = dnaUserArray[1];
		  
		  //System.out.print("dna: " + dna);
		  //System.out.print("user: " + user);
		  context.write(new Text(dna), new Text(user));
	  }
  }
}
