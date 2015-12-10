/**
 * Partitioner determines which characters to send to reducer 1 or 2
 **/
package dm.charcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CharCountPartitioner extends Partitioner<IntWritable, LongWritable>
{
	
	@Override
	public int getPartition(IntWritable key, LongWritable arg1, int arg2) {
		return key.get() % arg2;
		/***String keyS = key.toString();
		if((keyS.length() > 0) && (keyS.charAt(0) < 'k'))
		 {
			 return 0;
		 }
		 return 1;
		 ***/
		}
		

}
