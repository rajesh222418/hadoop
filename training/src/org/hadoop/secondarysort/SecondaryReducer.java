package org.hadoop.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
 
public class SecondaryReducer extends
        Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
	

	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context con)
			throws IOException, InterruptedException {
		//System.out.println(values.iterator().next().getFname());
		for(NullWritable name : values){
			//System.out.println(name.getFname()+""+name.getLname());
			con.write(key, name);
		}

	}
	 /* public void reduce(CompositeKeyWritable key,Iterable<NullWritable> values,Context context)
	            throws IOException, InterruptedException {
		
		  for(NullWritable value :values){
			  try {
				  
				  context.write(key, NullWritable.get());
			} catch (Exception e) {
				// TODO: handle exception
			}
		  }
		
	       
	    }*/
}