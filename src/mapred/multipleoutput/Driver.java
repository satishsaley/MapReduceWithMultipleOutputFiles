package mapred.multipleoutput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String args[]) throws Exception{
		Driver driver = new Driver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inDir = args[0];
		String outDir = args[1];

		Job job = new Job(getConf());
		job.setJarByClass(Driver.class);
		job.setJobName("Try multiple Output files");
		FileInputFormat.setInputPaths(job, new Path(inDir));
		FileOutputFormat.setOutputPath(job, new Path(outDir));

		job.setMapperClass(MultiOutMapper.class);
		job.setReducerClass(MultiOutReducer.class);

		// Defines additional single text based output 'text' for the job
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
				LongWritable.class, Text.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		if(!success)
			return -1;
		return 1;

	}
}
