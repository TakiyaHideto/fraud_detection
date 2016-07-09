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

/**
 * Created by TakiyaHideto on 15/12/29.
 */
public class CookieVolumeViaDomainIpPart2 {

    public static class CookieVolumeViaDomainIpPart2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CookieVolumeViaDomainIpMapper", "validInputCt");
            Counter invalidInputCt = context.getCounter("CookieVolumeViaDomainIpMapper","invalidInputCt");
            Counter validOutputCt = context.getCounter("CookieVolumeViaDomainIpMapper", "validOutputCt");
            Counter otherExceptionCt = context.getCounter("CookieVolumeViaDomainIpMapper", "otherExceptionCt");
            Counter protoBufferExceptCt = context.getCounter("CookieVolumeViaDomainIpMapper", "protoBufferExceptCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e) {
                otherExceptionCt.increment(1);
                return;
            }

            OriginalBidLog.User user = bidLog.getUser();
            OriginalBidLog.Page page = bidLog.getPage();

            long timestamp = Long.parseLong(String.valueOf(bidLog.getTimestamp()));
            String ip = user.getUserIp();
            String domain = UrlUtil.getUrlDomain(page.getPageUrl());
            String yoyiCookie = user.getUserYyid();
            String impressionHour = "";
            try{
                impressionHour = String.valueOf(DateUtil.getTimeOfHour(timestamp));
            } catch (Exception e){
                return;
            }

            // output key-value
            key_result.set(domain + Properties.Base.BS_SEPARATOR_UNDERLINE + ip);
            value_result.set(yoyiCookie +
                    Properties.Base.BS_SEPARATOR_TAB + domain +
                    Properties.Base.BS_SEPARATOR_TAB + impressionHour);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CookieVolumeViaDomainIpPart2Reducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CookieVolumeViaDomainIpReducer","validOutputCt");

            int impressionVolumeCounter = 0;

            HashSet<String> cookieSet = new HashSet<String>();
            HashSet<String> domainSet = new HashSet<String>();
            HashMap<String,String> hourMap = new HashMap<String,String>();

            for(Text value: values){
                String[] valueInfo = value.toString().split(Properties.Base.BS_SEPARATOR_TAB);
                cookieSet.add(valueInfo[0]);
                domainSet.add(valueInfo[1]);
                impressionVolumeCounter ++;
                if (hourMap.containsKey(valueInfo[2])){
                    String hourValue = hourMap.get(valueInfo[2]);
                    hourValue = String.valueOf(Integer.parseInt(hourValue)+1);
                    hourMap.remove(valueInfo[2]);
                    hourMap.put(valueInfo[2],hourValue);
                }
                else{
                    hourMap.put(valueInfo[2],"1");
                }
            }
            String cookieList = "";
            for (String cookieEle: cookieSet){
                cookieList += cookieEle + Properties.Base.BS_SUB_SEPARATOR;
            }
            String hourDistribution = "";
            for (String hourKey: hourMap.keySet()){
                hourDistribution += hourKey + ":" + hourMap.get(hourKey) + Properties.Base.BS_SUB_SEPARATOR;
            }
            if (cookieSet.size()>10) {
                value_result.set(key.toString() +
                        Properties.Base.BS_SEPARATOR + String.valueOf(cookieSet.size()) +
                        Properties.Base.BS_SEPARATOR + String.valueOf(impressionVolumeCounter) +
                        Properties.Base.BS_SEPARATOR + hourDistribution +
                        Properties.Base.BS_SEPARATOR + cookieList);
                context.write(NullWritable.get(), value_result);
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

        Job job = new Job(conf,"CookieVolumeViaDomainIpPart2");
        conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
        conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CookieVolumeViaDomainIpPart2.class);
        job.setReducerClass(CookieVolumeViaDomainIpPart2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogDataPath = otherArgs[1];
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        CommUtil.addInputFileCommSucc(job, fs, bidLogDataPath, TextInputFormat.class, CookieVolumeViaDomainIpPart2Mapper.class, "all");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
