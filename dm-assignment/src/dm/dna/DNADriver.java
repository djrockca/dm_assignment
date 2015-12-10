/** This map reduce program processes file that contains DNA sequence of people. It finds all the people who have same DNAs.
 
Input:
“User1 ACGT” 
“User2 TGCA”
“User3 ACG”
“User4 ACGT”
“User5 ACG”
“User6 AGCT”
 

Output: 
User1, User4
User2
User3, User 5
User6

Hadoop Command to run:
hadoop jar dm-assignment.jar dm.dna.DNADriver Assignments/input/dna.txt /user/djrockca4306/Assignments/mr/dna/output

 */
package dm.dna;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DNADriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: DNADriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = new Job(conf, "findSameDNAUser");
		job.setJarByClass(DNADriver.class);
		job.setMapperClass(DNAMapper.class);
		job.setReducerClass(DNAReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
