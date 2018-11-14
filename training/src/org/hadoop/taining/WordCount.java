package org.hadoop.taining;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	

	public int run(String[] args) throws Exception {		
        Job job = Job.getInstance(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1; 
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCount(), args));
	}

}