package com.mr.code_backup;
////

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;

public class DistributionMR{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        
        if(otherArgs.length != 2){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"DistributionMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME,Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER,Properties.Base.BS_HDFS_TRACKER);
		// job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        // job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		// job.getConfiguration().setBoolean("mapred.output.compress", true);
        // job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(DistributionMR.class);
		job.setReducerClass(DistributionReducer.DistributionStatReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		String dataInputPath = otherArgs[1];
		
        String hour = "all";	
        
		CommUtil.addInputFileComm(job,fs,dataInputPath, TextInputFormat.class, com.mr.code_backup.DistributionMapper.DistributionStatMapper.class, hour);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}