package org.hadoop.secondarysort;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, IntWritable >
{
@Override
public int getPartition(CompositeKeyWritable key, IntWritable value, int numReduceTasks) {
int hash = key.getFname().hashCode();
int partition = hash % numReduceTasks;
return partition;
}
}
