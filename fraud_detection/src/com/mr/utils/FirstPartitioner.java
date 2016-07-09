package com.mr.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;  
import java.lang.Math;
import java.util.Random;

public class FirstPartitioner extends Partitioner<TextLong,Text>{
	@Override  
	public int getPartition(TextLong key, Text value,   
		int numPartitions) {
			return Math.abs(key.getKey().hashCode() % numPartitions);
	}  
}  
