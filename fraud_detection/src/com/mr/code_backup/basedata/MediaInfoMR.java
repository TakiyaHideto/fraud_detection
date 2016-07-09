package com.mr.code_backup.basedata;

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
import com.mr.utils.DateUtil;

public class MediaInfoMR {

	/*----------------------------------------------------------------
	 * 函数名:main
	 * 函数功能:主函数
	 * 参数说明:
	 * otherArgs [0] : 日期
	 * otherArgs [1] : 是否使用空值 默认不用 Yes/No 用/不用
	 * otherArgs [2] : 是否使用作弊过滤  Yes/No     使用/不使用
	 * otherArgs [3] : campainId  内容   默认全部/未设置该参数 [expmple:3456] 
	 * 注: 如果使用后面的参数前面的参数要包含.
	------------------------------------------------------------------*/
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(Properties.Base.BS_DEFT_NAME,Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER,Properties.Base.BS_HDFS_TRACKER);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
		
		if (args.length < 4){
			System.out.println("please input four arguments");
			System.exit(-1);
		}
		
		/*************************** Media Job *********************************/
		Job jobMedia = new Job(conf, "MediaMRLm");
		FileSystem fsMedia = FileSystem.get(conf);
        
		jobMedia.getConfiguration().set("USE_NULL_FILTER",otherArgs[1]);
		jobMedia.getConfiguration().set("USE_CHEAT",otherArgs[2]);
		jobMedia.getConfiguration().set("USE_CAMPAINID",otherArgs[3]);
		
		// log_show输入目录
		String filePathsShow = Properties.Mapper.MP_IMPRESS_DIR + otherArgs[0];
		CommUtil.addInputFileComm(jobMedia, fsMedia, filePathsShow, TextInputFormat.class, MediaInfoMapper.class);

		// log_click输入目录
		String filePathsClick = Properties.Mapper.MP_CLICK_DIR + otherArgs[0];
		CommUtil.addInputFileComm(jobMedia, fsMedia, filePathsClick, TextInputFormat.class, MediaInfoMapper.class);
		
		// log_access 输入目录
        String filePathsAccess = Properties.Mapper.MP_SUCCESS_DIR + otherArgs[0];
        CommUtil.addInputFileComm(jobMedia, fsMedia,filePathsAccess, TextInputFormat.class, MediaInfoMapper.class);

		// log_reach输入目录
		String strCurrren = otherArgs[0];
		for (int i = 0; i < Properties.Comm.PC_CONV_DELAY_DAY_COUNT; ++i) {
			String filePathsReach = Properties.Mapper.MP_REACH_DIR + strCurrren;
			CommUtil.addInputFileComm(jobMedia, fsMedia, filePathsReach, TextInputFormat.class, MediaInfoMapper.class);
			strCurrren = DateUtil.getSpecifiedDayAfter(strCurrren);
		}
		
		String strOutPutDir = otherArgs[4];
		FileOutputFormat.setOutputPath(jobMedia, new Path(strOutPutDir));
		
		jobMedia.setJarByClass(MediaInfoMR.class);
		jobMedia.setMapperClass(MediaInfoMapper.class);
		jobMedia.setReducerClass(MediaInfoReducer.class);
		jobMedia.setMapOutputKeyClass(Text.class);
		jobMedia.setOutputKeyClass(NullWritable.class);
		jobMedia.setOutputValueClass(Text.class);
		jobMedia.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM);

		System.exit(jobMedia.waitForCompletion(true) ? 0 : -1);
	}
}
