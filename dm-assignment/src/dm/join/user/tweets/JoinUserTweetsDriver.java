package dm.join.user.tweets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinUserTweetsDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: JoinUserTweetsDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = new Job(conf, "joinusertweets");
		job.setJarByClass(JoinUserTweetsDriver.class);
		job.setReducerClass(JoinUserTweetsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		System.out.println("Paramter 1: " + args[0]);
		System.out.println("Paramter 2: " + args[1]);
		System.out.println("Paramter 3: " + args[2]);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinUserTweetsMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinUserTweetsMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
