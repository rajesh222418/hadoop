package org.hadoop.taining;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			word.set(token);
			context.write(word, ONE);
		}
	}
}
