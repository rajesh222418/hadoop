package org.hadoop.hightemp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static final int MISSING = 9999;

	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		String s1 = value.toString();
		String year = s1.substring(6,10);
		int temp = Integer.parseInt(s1.substring(40,45).replace(" ",""));

		con.write(new Text(year), new IntWritable(temp));

	}

}
