package com.mr.code_backup;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.lang.ArrayIndexOutOfBoundsException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.TextMessageCodec;

public class BenzDataMR{
	public static class BenzDataMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
        
        protected void setup(Context context) throws IOException,InterruptedException{
           	
        } 

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputAllCt = context.getCounter("benzDataMapper", "validInputAllCt");
            Counter validOutputCt = context.getCounter("benzDataMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("benzDataMapper", "protoBufferExceptCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("benzDataMapper", "arrayIndexOutOfBoundsExceptionCt");
            Counter dataHandlerExceptCt = context.getCounter("benzDataMapper", "dataHandlerExceptCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());  
                validInputAllCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            }

            //key
            String sessionId = bidLog.getSessionId();
            try{
                BenzDataHandler dataHandler = new BenzDataHandler();
                featureValueList = dataHandler.processBidLog(bidLog, context);
            } catch (ArrayIndexOutOfBoundsException e){
                arrayIndexOutOfBoundsExceptionCt.increment(1);
                return;
            } catch (ParseException e){
                dataHandlerExceptCt.increment(1);
                return;
            }

            // output key-value
            for (String fv : featureValueList){
                if (!fv.equals("")){
                    String[] infoElements = fv.split(Properties.Base.BS_SEPARATOR);
                    key_result.set(sessionId + Properties.Base.BS_SEPARATOR + infoElements[1]);
                    value_result.set(fv);
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            }      
		}
	}

	public static class BenzDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private MultipleOutputs multipleOutputs;
        
        protected void setup(Context context) throws IOException,InterruptedException{
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
            
            // initialize Counter
            Counter validOutputCt = context.getCounter("DiyuanxinDataReducer","validOutputCt");

            for (Text value: values){
                context.write(NullWritable.get(),value); 
                validOutputCt.increment(1); 
            }         
        }
    }

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"benzDataMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(DiyuanxinData.class);
		job.setReducerClass(BenzDataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM);
        

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String bidLogPath = otherArgs[1];
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileComm(job,fs,bidLogPath,TextInputFormat.class,BenzDataMapper.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}