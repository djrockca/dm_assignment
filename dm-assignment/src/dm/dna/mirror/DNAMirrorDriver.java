/** This map reduce program processes file that contains DNA sequence of people. It finds all the people who have same or mirror image of DNAs.
 
Input:

“User1 ACGT”
“User2 TGCA”
“User3 ACG”
“User4 ACGT”
“User5 ACG”

Output:
User1, User2, User4
User3, User 5

Hadoop Command to run:
hadoop jar dm-assignment.jar dm.dna.mirror.DNAMirrorDriver Assignments/input/dnamirror.txt /user/djrockca4306/Assignments/mr/dnamirror/output

 */
package dm.dna.mirror;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DNAMirrorDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: DNAMirrorDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = new Job(conf, "findSameMirrorDNAUser");
		job.setJarByClass(DNAMirrorDriver.class);
		job.setMapperClass(DNAMirrorMapper.class);
		job.setReducerClass(DNAMirrorReducer.class);
		
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
