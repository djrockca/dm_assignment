package dm.voting;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dm.customreader.NLinesInputFormat;

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
		//job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		//job.setInputFormatClass(NLinesInputFormat.class);
		
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
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
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
			if (jobCtrl.getFailedJobList().size() > 0) {
				System.out.println("Job Failed..." +jobCtrl.getFailedJobList().get(0).getJobName() + " with msg :" + jobCtrl.getFailedJobList().get(0).getMessage());
				System.exit(1);
			} else {
				System.out.println("Still running...");
				Thread.sleep(5000);
			}
		}
		

	}
}
