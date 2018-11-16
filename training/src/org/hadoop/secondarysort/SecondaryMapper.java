package org.hadoop.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SecondaryMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	/*public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		System.out.println(value.toString());
		String zipcode = null;
		String bike_id = null;
		try {
			zipcode = values[10];
			bike_id = values[8];
		} catch (Exception e) {
			// TODO: handle exception
		}

		if (null != zipcode || null != bike_id) {

			CompositeKeyWritable cw = new CompositeKeyWritable(zipcode, bike_id);

			try {

				context.write(cw, NullWritable.get());
			} catch (Exception e) {
				System.out.println(cw);
				// System.out.println(values[10]);
				System.out.println(values[8]);

				System.out.println("" + e.getMessage());

			}

		}

	}*/
	CompositeKeyWritable flname = new CompositeKeyWritable();
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		String[] str = value.toString().split(",");
		int age = Integer.parseInt(str[2]);
		flname.setAge(new IntWritable(age));
		flname.setFname(new Text(str[0]));
		flname.setLname(new Text(str[1]));
		//System.out.println(flname.getFname());
		

		con.write(flname, NullWritable.get());

	}

}