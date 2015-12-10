/**This map reduce program finds anagrams in a huge text. 
 * An anagram is basically a different arrangement of letters in a word. Anagram does not need to be meaningful.
 * Input: “the cat act in tic tac toe.”
 * Output: cat, tac, act
 * 
 *  Command to run on hadoop: 
 * hadoop jar dm-assignment.jar dm.anagram.AnagramDriver Assignments/input/anagram.txt /user/djrockca4306/Assignments/mr/anagram/output
***/
package dm.anagram;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dm.anagram.AnagramMapper;
import dm.anagram.AnagramReducer;

public class AnagramDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: AnagramDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = new Job(conf, "findanagrams");
		job.setJarByClass(AnagramDriver.class);
		job.setMapperClass(AnagramMapper.class);
		job.setReducerClass(AnagramReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
