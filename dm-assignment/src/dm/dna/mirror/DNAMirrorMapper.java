package dm.dna.mirror;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.lang.StringUtils;

public class DNAMirrorMapper extends Mapper<Object, Text, Text, Text> {

	  @Override
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	  
	  //System.out.print("value: " + value);
	  String[] dnaUsers = value.toString().split("\n");
	  String dna = new String();
	  String user = new String();
	  String reversedDNA =  new String();
	  for(String dnaUser:dnaUsers)
	  {
		  //convert the word to lower case
		  dnaUser = dnaUser.toLowerCase();
		 // System.out.print("dnaUser: " + dnaUser + "\n");
		  String[] dnaUserArray= dnaUser.split("[ \t]+");
		  
		  user = dnaUserArray[0];
		  dna = dnaUserArray[1];
		  reversedDNA = StringUtils.reverse(dna);
		  if (dna.compareTo(reversedDNA) > 0 ) {
			  dna = reversedDNA;
		  } 
		 // System.out.print("dna: " + dna +  ", reversedDNA :" + reversedDNA + ", user :" + user + "\n");
		  context.write(new Text(dna), new Text(user));
	  }
  }
}
