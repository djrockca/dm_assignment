/**
 * Map reduce program to compute voting worth
 * 
 * Problem Statement:
 * In an unusual democracy, everyone is not equal. The vote count is a function of worth of the voter. Though everyone is voting for each other.
 * Find out what is the vote count of everyone?
 *
 *
 * As example, if A with a worth of 5 and B with a worth of 1 are voting  for C, the vote count of C would be 6.
 * You are given a list of people with their value of vote. You are also given  another list describing who voted for who all. 
 *
 *
 * 	List1
 *
 *	Voter Votee
 *
 *	A 		C
 *  B 		C
 *  C 		F
 *  A 		B
 *
 *  List2 
 *  
 *  Person Worth
 *	A 		5
 *  B 		1
 *  C 		11
 *  D 		12
 * 
 * Result
 * Person VoteCount
 * 
 * B 		5
 * C 		6
 * F 		11
 * 
 * Command to run
 * hadoop jar dm-assignment.jar dm.voting.VotingDriver /user/djrockca4306/Assignments/input/voting /user/djrockca4306/Assignments/mr/voting/output
 */

 /**
  * This solution uses 2 reduce phases to solve the problem.  
  * In the first phase the mapper mergers the 2 input list.
  * Hadoop provides reducer values (votee and worth) grouped by key (voter)
  * Reducer1 computes computes votee worth 
  * In phase 2 , the reducer sums up the worth and emits the output
  * The jobs are chained and only after first map-reduce job finishes, can the second one start.
  */
package dm.voting;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class VotingDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: VotingDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		JobControl jobCtrl = new JobControl("votinggrp");
		
		Job job1 = Job.getInstance(conf, "votingjob1");
		job1.setJarByClass(VotingDriver.class);
		job1.setMapperClass(VotingMapper1.class);
		job1.setReducerClass(VotingReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path tmpDir = new Path(args[1] + "-tmp");
		FileOutputFormat.setOutputPath(job1, tmpDir);
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);

		Job job2 = Job.getInstance(conf, "votingjob2");
		job2.setJarByClass(VotingDriver.class);
		job2.setMapperClass(VotingMapper2.class);
		job2.setReducerClass(VotingReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		job2.setNumReduceTasks(1);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2, tmpDir);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2);
		cJob2.addDependingJob(cJob1);

		jobCtrl.addJob(cJob1);
		jobCtrl.addJob(cJob2);

		Thread thread = new Thread(jobCtrl);
		thread.start();
			while (!jobCtrl.allFinished()) {
				System.out.println("Still running jobs...");
				Thread.sleep(5000);
		}
		
		if (jobCtrl.allFinished()) {
			System.out.println("Number of Jobs Successful :  " + jobCtrl.getSuccessfulJobList().size());
			System.out.println("Number of Jobs Failed :  " + jobCtrl.getFailedJobList().size());
			System.out.println("Process complete");
			System.exit(1);
		}
	}
}
