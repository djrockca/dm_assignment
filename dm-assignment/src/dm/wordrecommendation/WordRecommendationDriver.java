/**
 * Map reduce program to formulate the next word recommendation based on the content from a very large text archive
 *
 *	For each word, prepare a top 5 recommendations of the words that would go next.
 *	e.g. 
 *	happy 	birthday, newyear, marriage
 *  how 	are, do, did
 *	
 *  Command to run:
 *	hadoop jar dm-assignment.jar dm.wordrecommendation.WordRecommentationDriver Dilshad/input/big.txt Dilshad/WordRecommendation/output
 *
 * 
 */
package dm.wordrecommendation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordRecommendationDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: WordRecommendationDriver <input dir> <output dir>\n");
			System.exit(-1);
		} 
		JobConf conf = new JobConf();
		Job job = new Job(conf, "findWordRecommendations");
		job.setJarByClass(WordRecommendationDriver.class);
		job.setMapperClass(WordRecommendationMapper.class);
		job.setReducerClass(WordRecommendationReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		conf.set("textiputformat.record.delimeter", ".");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
