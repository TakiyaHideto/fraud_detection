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
import com.mr.protobuffer.OriginalClickLog;
import com.mr.utils.CommUtil;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.TextMessageCodec;


public class BenzDetectionClickJoinMR {
	public static class BenzDetectionMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validOutputCt = context.getCounter("BenzDetectionMapper", "validOutputCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            String line = value.toString().trim();
            String[] elementInfo = line.split(Properties.Base.BS_SEPARATOR);

            //key
            String yoyi_cookie = elementInfo[0];

            // output key-value
            key_result.set(yoyi_cookie);
            // value_result.set(line);
            value_result.set("detectionLog");
            context.write(key_result, value_result);
            validOutputCt.increment(1);      
		}
	}

    public static class BenzDetectionClickLogMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

        protected void setup(Context context) throws IOException,InterruptedException{

        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("BenzDetectionClickLogMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("BenzDetectionClickLogMapper", "validOutputCt");
            Counter otherExceptionCt = context.getCounter("BenzDetectionClickLogMapper", "otherExceptionCt");
            Counter protoBufferExceptCt = context.getCounter("BenzDetectionClickLogMapper", "protoBufferExceptCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalClickLog.ClickLogMessage clickLog;
            try{
                clickLog = (OriginalClickLog.ClickLogMessage) TMC.parseFromString(value.toString(), OriginalClickLog.ClickLogMessage.newBuilder());  
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            //key
            String yoyi_cookie = AuxiliaryFunction.extractYoyiCookie(clickLog.getData());
            //value
            String timestamp = String.valueOf(clickLog.getTimestamp()); 
            String campaignId = AuxiliaryFunction.extractCampaignId(clickLog.getData());
            String orderId = AuxiliaryFunction.extractOrderId(clickLog.getData());

            // output key-value
            if (campaignId.equals("270")){
                // key_result.set(yoyi_cookie);
                // value_result.set("clickTime:" + timestamp);
                key_result.set(yoyi_cookie);
                value_result.set(orderId);
                context.write(key_result, value_result);
                validOutputCt.increment(1); 
            }                 
        }
    }

	public static class BenzDetectionClickJoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter actionCt = context.getCounter("BenzDetectionClickJoinReducer","actionCt");
            Counter validOutputCt = context.getCounter("BenzDetectionClickJoinReducer","validOutputCt");
            Counter multipleClickNumCt = context.getCounter("BenzDetectionClickJoinReducer","multipleClickNumCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("BenzDetectionClickJoinReducer", "arrayIndexOutOfBoundsExceptionCt");
            Counter order3404Ct = context.getCounter("BenzDetectionClickJoinReducer","order3404Ct");
            Counter order3476Ct = context.getCounter("BenzDetectionClickJoinReducer","order3476Ct");
            Counter crossCookieCt = context.getCounter("BenzDetectionClickJoinReducer","crossCookieCt");

            Boolean hasDetectionInfo = false;
            Boolean hasClickInfo = false;
            Boolean firstOrder = false;
            Boolean secondOrder = false;
            Boolean isDetectionLog = false;
            String detectionInfo = "";
            String clickTimestamp = "";

            int multipleClickNumCounter = 0;

            // 0: yoyi_cookie string 
            // 1: accessTimestamp <string>
            // 2: actionTimestamp <string>
            // 3: registerTimestamp <string>  
            // 4: clossTimestamp <string>
            // 5: accessReferer <string> 
            // 6: actionReferer <string> 
            // 7: userAgent string 
            // 8: browserLanguage string
            // 9: accessFlag string
            // 10: actionFlag string
            // 11: registerFlag string
            // 12: clossFlag string

            for (Text value: values){
                String valueString = value.toString();
            	if (valueString.contains("clickTime")){
                    multipleClickNumCounter ++;
                    clickTimestamp = valueString.substring("clickTime:".length(),valueString.length()) + Properties.Base.BS_SUB_SEPARATOR + clickTimestamp;
                    hasClickInfo = true;
                } else {  
                    hasDetectionInfo = true;
                    detectionInfo = valueString;
                }
            }

            if(multipleClickNumCounter>1){
                multipleClickNumCt.increment(1);
            }

            if(hasDetectionInfo && hasClickInfo){
                if (detectionInfo.contains("action")){
                    actionCt.increment(1);
                }     
                value_result.set(detectionInfo + Properties.Base.BS_SEPARATOR + clickTimestamp);
                context.write(NullWritable.get(),value_result); 
                validOutputCt.increment(1);
            } 

            // for (Text value: values){
            //     String valueString = value.toString();
            //     if (valueString.equals("3404")){
            //         firstOrder = true;
            //     }
            //     else if (valueString.equals("3476")){
            //         secondOrder = true;
            //     }
            //     else if (valueString.equals("detectionLog")){
            //         isDetectionLog = true;
            //     }
            // }
            // if (firstOrder && isDetectionLog)
            //     order3404Ct.increment(1);
            // if (secondOrder && isDetectionLog)
            //     order3476Ct.increment(1);

            // if (firstOrder && secondOrder && isDetectionLog){
            //     crossCookieCt.increment(1);
            //     context.write(NullWritable.get(),key);
            // }        
        }
    }

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 3){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"BenzDetectionClickJoinMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		// job.getConfiguration().setBoolean("mapred.compress.map.output", true);
  //       job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		// job.getConfiguration().setBoolean("mapred.output.compress", true);
  //       job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(BenzDetectionClickJoinMR.class);
		job.setReducerClass(BenzDetectionClickJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(20);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String benzCampLog = otherArgs[1];
        String clickLogPath = otherArgs[2];
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileCommSpecialJob(job,fs,benzCampLog,TextInputFormat.class,BenzDetectionMapper.class, "diyuanxin");
        CommUtil.addInputFileComm(job,fs,clickLogPath,TextInputFormat.class,BenzDetectionClickLogMapper.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}