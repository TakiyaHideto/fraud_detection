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
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalReachLog;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.UrlUtil;
import com.mr.utils.CommUtil;
import com.mr.utils.TextMessageCodec;

public class CheckReachVolumeMR{

	public static class CheckReachVolumeReachMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();    
		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("CheckReachVolumeReachMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CheckReachVolumeReachMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("CheckReachVolumeReachMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckReachVolumeReachMapper","otherExceptionCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalReachLog.ArrivalLogMessage reachLog;
            try{
                reachLog = (OriginalReachLog.ArrivalLogMessage) TMC.parseFromString(value.toString(), OriginalReachLog.ArrivalLogMessage.newBuilder());  
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            String sessionId = AuxiliaryFunction.extractSessionId(reachLog.getData());
            String orderId = AuxiliaryFunction.extractOrderId(reachLog.getData());

            // output key-value
            key_result.set(sessionId + orderId);
            value_result.set("reachLog");
            context.write(key_result, value_result);
            validOutputCt.increment(1);      
		}
	}

	public static class CheckReachVolumeBidMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("CheckReachVolumeBidMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CheckReachVolumeBidMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("CheckReachVolumeBidMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckReachVolumeBidMapper","otherExceptionCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());  
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            //key
            String sessionId = bidLog.getSessionId();

            // output key-value
            for (OriginalBidLog.Ad ad : bidLog.getAdsList()){
            	// getUrlDomain
            	if(UrlUtil.getUrlDomain(bidLog.getPage().getPageUrl()).contains("huanqiu.com") && ad.getCampaignId().equals("220")){
            		value_result.set("bidLog");
            		key_result.set(sessionId + ad.getOrderId());
            		context.write(key_result, value_result);
            		validOutputCt.increment(1); 
            	}
            }     
		}
	}

	public static class CheckReachVolumeReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter reachVolumeCt = context.getCounter("CheckReachVolumeReducer","reachVolumeCt");
            Boolean isReachLog = false;
            Boolean isBidLog = false;

            for (Text value: values){
                if(value.toString().contains("bidLog")){
                	isBidLog = true;
                }
                else if(value.toString().contains("reachLog")){
                	isReachLog = true;
                }
            }
            if(isBidLog && isReachLog){
            	value_result.set("-");
            	context.write(NullWritable.get(),value_result);
            	reachVolumeCt.increment(1);
            }         
        }
    }

    public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 3){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"CheckReachVolumeMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(CheckReachVolumeMR.class);
		job.setReducerClass(CheckReachVolumeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String bidLogPath = otherArgs[1];
		String reachLogPath = otherArgs[2];
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileComm(job,fs,bidLogPath,TextInputFormat.class,CheckReachVolumeBidMapper.class,"all");
		CommUtil.addInputFileComm(job,fs,reachLogPath,TextInputFormat.class,CheckReachVolumeReachMapper.class,"all");
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

}









