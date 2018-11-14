package org.hadoop.taining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int reduceTasks) {
		if(key.compareTo(new Text("i")) < 0){
			return 0;
		}else if(key.compareTo(new Text("r")) < 0){
			return 1;
		}else{
			return 2;
		}
	}

}
