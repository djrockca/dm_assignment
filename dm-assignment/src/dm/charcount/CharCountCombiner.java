package dm.charcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountCombiner extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> 
{
	@Override
	  public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
	      throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable lw:values)
		{
			sum += lw.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
