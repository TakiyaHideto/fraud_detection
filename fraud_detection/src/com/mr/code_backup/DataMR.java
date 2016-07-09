package com.mr.code_backup;

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

public class DataMR{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 7){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"ExtractAllBidLog"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(DataMR.class);
		job.setReducerClass(DataReducer.LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM+10);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String filePathBid = otherArgs[1];
		String filePathWin = otherArgs[2];
		String filePathShow = otherArgs[3];
		String filePathclick = otherArgs[4];
		String importPath = otherArgs[5];
		String orderTanxPath = otherArgs[6];

		job.getConfiguration().set("importPath", importPath);
		job.getConfiguration().set("orderTanxPath", orderTanxPath);
		job.getConfiguration().set("mapred.queue.name", "algo-dev");
        String hour = "all";	
        
		CommUtil.addInputFileComm(job,fs,filePathBid, TextInputFormat.class, com.mr.code_backup.DataMapper.OriginalBidLogMapper.class, hour);
		// CommUtil.addInputFileComm(job,fs,filePathWin, TextInputFormat.class, DataMapper.OriginalWinLogMapper.class, hour);
		// CommUtil.addInputFileComm(job,fs,filePathShow, TextInputFormat.class, DataMapper.OriginalShowLogMapper.class, hour);
		// CommUtil.addInputFileComm(job,fs,filePathclick, TextInputFormat.class, DataMapper.OriginalClickLogMapper.class, hour);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}