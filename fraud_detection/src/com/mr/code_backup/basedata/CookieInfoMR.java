package com.mr.code_backup.basedata;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;

public class CookieInfoMR {

	/*----------------------------------------------------------------
	* 函数名:main
	* 函数功能:主函数
	* 参数说明:
	------------------------------------------------------------------*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

		if (args.length < 2){
			System.out.println("please input two arguments");
			System.exit(-1);
		}

		String filePathsMedia = otherArgs[0];
		String strCoutPutDir = otherArgs[1];
		
		/***************************Cookie Job*********************************/
		Job jobCookie = new Job(conf, "CookieMRLm");
		FileSystem fsCookie = FileSystem.get(conf);	
		
		// Media输入目录
		CommUtil.addInputFileComm(jobCookie,fsCookie,filePathsMedia, TextInputFormat.class, CookieInfoMapper.class);
		
		// Cookie输入目录
		Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
        
		String filePathsCookie = Properties.Mapper.MP_COOKIE_INFO_PATH + yesterday;
		CommUtil.addInputFileComm(jobCookie,fsCookie,filePathsCookie, TextInputFormat.class, CookieInfoMapper.class);
		
		FileOutputFormat.setOutputPath(jobCookie, new Path(strCoutPutDir));
		
		jobCookie.setJarByClass(CookieInfoMR.class);
		jobCookie.setMapperClass(CookieInfoMapper.class);
		jobCookie.setReducerClass(com.mr.code_backup.basedata.CookieInfoReducer.class);
		jobCookie.setMapOutputKeyClass(Text.class);
		jobCookie.setOutputKeyClass(NullWritable.class);
		jobCookie.setOutputValueClass(Text.class);
		jobCookie.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM);
		
		System.exit(jobCookie.waitForCompletion(true) ? 0 : -1);
	}
}
