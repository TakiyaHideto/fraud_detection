package com.mr.code_backup;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;


public class BenzLandingPageInfoMR {
	public static class BenzLandingPageInfoMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
        
        protected void setup(Context context) throws IOException,InterruptedException{
           	
        } 

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validOutputCt = context.getCounter("benzDataMapper", "validOutputCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            String line = value.toString().trim();
            String[] elementInfo = line.split(Properties.Base.BS_SEPARATOR);

            // 0 日志版本 1.0.0
			// 1 日志类型 1:Access 2: Action 3:AdShow 4:AdClick
			// 2 请求时间
			// 3 用户ID
			// 4 客户端IP
			// 5 UserAgent
			// 6 Referer
			// 7 浏览器语言
			// 8 浏览器编码
			// 9 请求方法 0：GET 1：POST
			// 10 请求参数 监测请求串（UrlEncode）
			// 11 Cookie值

            //key
            String yoyi_cookie = elementInfo[3];

            // output key-value
            key_result.set(yoyi_cookie);
            value_result.set(line);
            context.write(key_result, value_result);
            validOutputCt.increment(1);      
		}
	}

	public static class BenzLandingPageInfoReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validOutputCt = context.getCounter("BenzLandingPageInfoReducer","validOutputCt");
            Counter accessVolumeCt = context.getCounter("BenzLandingPageInfoReducer","accessVolumeCt");
            Counter timestampExceptionCt = context.getCounter("BenzLandingPageInfoReducer","timestampExceptionCt");
            Counter detectionTypeErrorExceptionCt = context.getCounter("BenzLandingPageInfoReducer","detectionTypeErrorExceptionCt");

            String accessTimestamp = "";
            String actionTimestamp = "";
            String registerTimestamp = "";
            String clossTimestamp = "";
            String userAgent = "";
            String accessReferer = "";
            String actionReferer = "";
            String browserLanguage = "";
            String accessFlag = "";
            String actionFlag = "";
            String registerFlag = "";
            String clossFlag = "";

            for (Text value: values){
            	String[] elementInfo = value.toString().split(Properties.Base.BS_SEPARATOR);
            	String logType = elementInfo[10].split("&")[0].split("=")[1];
                if (logType.equals("Access")){
                	accessTimestamp += elementInfo[2] + Properties.Base.BS_SUB_SEPARATOR;
                    accessReferer += elementInfo[6] + Properties.Base.BS_SEPARATOR_UNDERLINE;
                    accessVolumeCt.increment(1);
                }
                else if (logType.equals("Action")){
                    if (elementInfo[10].contains("act=0000002")){
                        registerTimestamp += elementInfo[2] + Properties.Base.BS_SUB_SEPARATOR;
                    }
                    else{
                        actionTimestamp += elementInfo[2] + Properties.Base.BS_SUB_SEPARATOR;
                        actionReferer += elementInfo[6] + Properties.Base.BS_SEPARATOR_UNDERLINE;
                    }
                }
                else if (logType.equals("Close")) {
                	clossTimestamp += elementInfo[2] + Properties.Base.BS_SUB_SEPARATOR;
                }
                else{
                    detectionTypeErrorExceptionCt.increment(1);
                }
                userAgent = elementInfo[5];
                browserLanguage = elementInfo[7];
            }

            try{
                if (!accessTimestamp.equals("")){
                    accessTimestamp = accessTimestamp.substring(0,accessTimestamp.length()-Properties.Base.BS_SUB_SEPARATOR .length());
                    accessFlag = "access";
                }
                if (!actionTimestamp.equals("")){
                    actionTimestamp = actionTimestamp.substring(0,actionTimestamp.length()-Properties.Base.BS_SUB_SEPARATOR .length());
                    actionFlag = "action";
                }
                if (!registerTimestamp.equals("")){
                    registerTimestamp = clossTimestamp.substring(0,registerTimestamp.length()-Properties.Base.BS_SUB_SEPARATOR .length());
                    registerFlag = "register";
                }
                if (!clossTimestamp.equals("")){
                    clossTimestamp = clossTimestamp.substring(0,clossTimestamp.length()-Properties.Base.BS_SUB_SEPARATOR .length());
                    clossFlag = "Close";
                }                 
            } catch (Exception e){
                timestampExceptionCt.increment(1);
            }

            value_result.set(key.toString() + 
            	Properties.Base.BS_SEPARATOR + accessTimestamp + 
            	Properties.Base.BS_SEPARATOR + actionTimestamp +
                Properties.Base.BS_SEPARATOR + registerTimestamp +  
            	Properties.Base.BS_SEPARATOR + clossTimestamp +
                Properties.Base.BS_SEPARATOR + accessReferer + 
                Properties.Base.BS_SEPARATOR + actionReferer + 
                Properties.Base.BS_SEPARATOR + userAgent + 
                Properties.Base.BS_SEPARATOR + browserLanguage +
                Properties.Base.BS_SEPARATOR + accessFlag + 
                Properties.Base.BS_SEPARATOR + actionFlag + 
                Properties.Base.BS_SEPARATOR + registerFlag + 
                Properties.Base.BS_SEPARATOR + clossFlag);
            context.write(NullWritable.get(),value_result); 
            validOutputCt.increment(1);         
        }
    }

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"BenzLandingPageInfoMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		// job.getConfiguration().setBoolean("mapred.compress.map.output", true);
  //       job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		// job.getConfiguration().setBoolean("mapred.output.compress", true);
  //       job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(BenzLandingPageInfoMR.class);
		job.setReducerClass(BenzLandingPageInfoMR.BenzLandingPageInfoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String benzCampLog = otherArgs[1];
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileComm(job,fs,benzCampLog,TextInputFormat.class,BenzLandingPageInfoMR.BenzLandingPageInfoMapper.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}