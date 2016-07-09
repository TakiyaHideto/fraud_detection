package com.mr.code_backup;
////

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;

public class RatioStatMR{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        
        if(otherArgs.length != 2){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"RatioStatMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME,Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER,Properties.Base.BS_HDFS_TRACKER);
        // conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		FileSystem fs = FileSystem.get(conf);
		job.setJarByClass(com.mr.code_backup.RatioStatMapper.RatioStatFraudMapper.class);
		job.setReducerClass(RatioStatReducer.RatioStatFraudReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(30);
		job.getConfiguration().set("mapred.queue.name", "algo-dev");
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String filePath = otherArgs[1];

		CommUtil.addInputFileComm(job,fs,filePath, TextInputFormat.class, com.mr.code_backup.RatioStatMapper.RatioStatFraudMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}