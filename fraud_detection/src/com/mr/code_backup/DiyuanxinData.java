package com.mr.code_backup;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.ArrayIndexOutOfBoundsException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.TextMessageCodec;

public class DiyuanxinData{
	// 包括cookie mapping和整理cookie兴趣标签
	public static class DiyuanxinDataMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
        private char[] encode = new char[256];
        private char[] decode = new char[256];
        private HashMap<String,String> cookieCodeMapping = new HashMap<String,String>();
        
        protected void setup(Context context) throws IOException,InterruptedException{
           	encode = "================================================klmnopqrst=======UVWXYZ0123456789abcdefghij======uvwxyzABCDEFGHIJKLMNOPQRST=====================================================================================================================================".toCharArray();
           	decode = "================================================GHIJKLMNOP=======ghijklmnopqrstuvwxyzABCDEF======QRSTUVWXYZ0123456789abcdef=====================================================================================================================================".toCharArray();
           	// for(int i=0;i<code1.length;i++){
           	// 	this.cookieCodeMapping.put(code1[i],code2[i]);
           	// }
        } 

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter failDecodeCt = context.getCounter("DiyuanxinDataMapper", "failDecodeCt");
            Counter validOutputCt = context.getCounter("DiyuanxinDataMapper", "validOutputCt");
            Counter brokenDataItemCt = context.getCounter("DiyuanxinDataMapper","brokenDataItemCt");
            Counter wrongCodeCt = context.getCounter("DiyuanxinDataMapper", "wrongCodeCt");
            Counter illegalCookieCt = context.getCounter("DiyuanxinDataMapper","illegalCookieCt");
            Counter wrongOriginCookieCt = context.getCounter("DiyuanxinDataMapper","wrongOriginCookieCt");
            ArrayList<Character> cookieDecode = new ArrayList<Character>();

            // initialization
            String line = value.toString().trim();
            String[] infoElements = line.split("\t");
            char[] cookieEncode = infoElements[0].toCharArray();

            if (infoElements[0].length()==21)
                ;
            else if (infoElements[0].length()==33)
                ;
            else {
                wrongOriginCookieCt.increment(1);
                return;
            }

            // 转码
            String yoyiCookie = "";
            for(int i=0;i<cookieEncode.length;i++){
                if (cookieEncode[i]=='='){
                    yoyiCookie += "-";
                }
                else
                    yoyiCookie += (String.valueOf(this.decode[(int)cookieEncode[i]]));
            }

            if (yoyiCookie.length()!=infoElements[0].length()){
                illegalCookieCt.increment(1);
                return;
            }
            if (yoyiCookie.length()==21 || yoyiCookie.length()==31){
                wrongCodeCt.increment(1);
            }

            // output key-value
            try{
                String dateTime = infoElements[2];
                String date = dateTime.split(" ")[0];
                String hour = dateTime.split(" ")[1].split(":")[0];
                String min = dateTime.split(" ")[1].split(":")[1];
                key_result.set(yoyiCookie);
                value_result.set(yoyiCookie + 
//                    Properties.Base.BS_SEPARATOR + infoElements[0] +
                    Properties.Base.BS_SEPARATOR + infoElements[1] +
                    Properties.Base.BS_SEPARATOR + date +
                    Properties.Base.BS_SEPARATOR + hour +
                    Properties.Base.BS_SEPARATOR + min +
                    Properties.Base.BS_SEPARATOR + "detection");
                context.write(key_result, value_result);
                validOutputCt.increment(1); 
            } catch(ArrayIndexOutOfBoundsException e) {
                brokenDataItemCt.increment(1);
            }
		}
	}

    public static class OriginalBidLogMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("OriginalBidLogMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("OriginalBidLogMapper", "validOutputCt");
            Counter otherExceptionCt = context.getCounter("OriginalBidLogMapper", "otherExceptionCt");
            Counter protoBufferExceptCt = context.getCounter("OriginalBidLogMapper", "protoBufferExceptCt");
            Counter dataHandlerExceptCt = context.getCounter("OriginalBidLogMapper", "dataHandlerExceptCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("OriginalBidLogMapper", "arrayIndexOutOfBoundsExceptionCt");

            // initialization
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

            // key info 
            String yoyiCookie = bidLog.getUser().getUserYyid();

            // value info
            String bidLogTag = "bidLog";

            // output key-value
            key_result.set(yoyiCookie);
            value_result.set(bidLogTag);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }

    }

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if(otherArgs.length != 3){  
            System.err.println("<int> <out>");  
            System.exit(4);
        }
 
        Job job = new Job(conf,"DiyuanxinDataMR"); 
		conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
		conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		FileSystem fs = FileSystem.get(conf);
        
		job.setJarByClass(DiyuanxinData.class);
		job.setReducerClass(DiyuanxinReducer.DiyuanxinDataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Properties.Base.BS_REDUCER_NUM+30);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		String diyuanxiDataPath = otherArgs[1];
        String bidLogPath = otherArgs[2];
        String hour = "all";
		job.getConfiguration().set("mapred.queue.name", "algo-dev");	
        
		CommUtil.addInputFileComm(job,fs,diyuanxiDataPath, TextInputFormat.class, DiyuanxinDataMapper.class);
        CommUtil.addInputFileComm(job,fs,bidLogPath, TextInputFormat.class, OriginalBidLogMapper.class,hour);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}