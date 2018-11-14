package org.hadoop.taining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static final Logger logger = Logger.getLogger(WordCountReducer.class);
	
	private IntWritable count = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			logger.info(value);
			sum += value.get();
		}
		count.set(sum);
		context.write(key, count);
	}
}
