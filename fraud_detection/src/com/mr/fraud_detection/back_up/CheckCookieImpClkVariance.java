package com.mr.fraud_detection.back_up;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.TextMessageCodec;
import com.mr.utils.UrlUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/1/12.
 */
public class CheckCookieImpClkVariance {
    public static class CheckCookieImpClkVarianceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckCookieImpClkVarianceMapper", "validInput");
            Counter protoBufferExceptCt = context.getCounter("CheckCookieImpClkVarianceMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckCookieImpClkVarianceMapper","otherExceptionCt");
            Counter nullCookieInfoCt = context.getCounter("CheckCookieImpClkVarianceMapper", "nullCookieInfoCt");

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

            String yoyiCookie = bidLog.getUser().getUserYyid();
            if (yoyiCookie.equals("")) {
                nullCookieInfoCt.increment(1);
                return;
            }
            long timestamp = bidLog.getTimestamp();
            String ip = bidLog.getUser().getUserIp();
            String domain = UrlUtil.getUrlDomain(bidLog.getPage().getPageUrl());
            String hour = "";
            try {
                hour = String.valueOf(DateUtil.getTimeOfHour(timestamp));
            } catch (Exception e){
                return;
            }

            // output key-value
            // key_result.set(yoyi_cookie);
            key_result.set(ip);
            value_result.set(hour + Properties.Base.BS_SEPARATOR + domain);
            context.write(key_result, value_result);
        }
    }

    public static class CheckCookieImpClkVarianceReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckCookieImpClkVarianceReducer", "validOutputCt");

            HashMap<String,Integer> hourMap = new HashMap<String, Integer>();
            HashSet<String> domainSet = new HashSet<String>();
            LinkedList<String> hourList = new LinkedList<String>();
            int thresholdCounter = 0;
            for (Text value: values){
                thresholdCounter ++;
                String[] elementInfo = value.toString().split(Properties.Base.BS_SEPARATOR,-1);
                domainSet.add(elementInfo[1]);
                if (hourMap.containsKey(elementInfo[0])){
                    int volume = hourMap.get(elementInfo[0]);
                    volume ++;
                    hourMap.remove(elementInfo[0]);
                    hourMap.put(elementInfo[0],volume);
                } else {
                    hourMap.put(elementInfo[0],1);
                }
                hourList.add(elementInfo[0]);
                if (thresholdCounter>1000){
                    context.write(NullWritable.get(), key);
                    return;
                }
            }

            double tempSum = 0.0;
            int tempCountSum = 0;
            for(String hourKey: hourMap.keySet()){
                int hourInt = Integer.valueOf(hourKey);
                tempSum += hourInt*hourMap.get(hourKey);
                tempCountSum += hourMap.get(hourKey);
            }
            double hourMean = tempSum/(double)tempCountSum;

            tempSum = 0.0;
            int impressionSum = 0;
            for (String hourEle: hourList){
                tempSum += Math.pow((Double.parseDouble(hourEle)-hourMean),2);
                impressionSum ++;
            }
            double variance = Math.sqrt(tempSum/(double)impressionSum);
            double result = (double)domainSet.size()*variance/impressionSum;
            if (impressionSum>50){
                value_result.set(key.toString() +
                        Properties.Base.BS_SEPARATOR + String.valueOf(variance) +
                        Properties.Base.BS_SEPARATOR + String.valueOf(impressionSum) +
                        Properties.Base.BS_SEPARATOR + String.valueOf(domainSet.size()) +
                        Properties.Base.BS_SEPARATOR + String.valueOf(result));
                if (result<0.01) {
                    context.write(NullWritable.get(), value_result);
                    validOutputCt.increment(1);
                }
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

        Job job = new Job(conf,"CheckCookieImpClkVariance");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckCookieImpClkVariance.class);
        job.setReducerClass(CheckCookieImpClkVarianceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, bidLogFile, TextInputFormat.class, CheckCookieImpClkVarianceMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
