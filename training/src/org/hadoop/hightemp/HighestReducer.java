package org.hadoop.hightemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context con)
			throws IOException, InterruptedException {
		int max_temp = 0;
		for (IntWritable value : values) {
			int current = value.get();
			if (max_temp < current){
				max_temp = current;
			}
		}
		con.write(key, new IntWritable(max_temp));

	}

}
