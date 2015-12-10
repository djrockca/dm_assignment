/** This map reduce program finds frequency of characters in a huge text
 * The mapper receives one line as input until all lines form huge text are processed.
 * Reducer finds the frequency count
 * 
 * Command to run on hadoop
 * hadoop jar dm-assignment.jar dm.charcount.CharCountDriver Assignments/input/big.txt /user/djrockca4306/Assignments/mr/charcount/output
 * 
 */
package dm.charcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import dm.charcount.StubCombiner;

import dm.charcount.CharCountMapper;
//import dm.charcount.StubPartitioner;
import dm.charcount.CharCountReducer;
import dm.customreader.NLinesInputFormat;

public class CharCountDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: CharCountDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = Job.getInstance(conf, "charcount");
		job.setJarByClass(CharCountDriver.class);
		job.setMapperClass(CharCountMapper.class);
		job.setReducerClass(CharCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setPartitionerClass(CharCountPartitioner.class);
		job.setCombinerClass(CharCountCombiner.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(NLinesInputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
