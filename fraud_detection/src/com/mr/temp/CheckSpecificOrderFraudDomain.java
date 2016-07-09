package com.mr.temp;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.utils.*;
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
 * Created by TakiyaHideto on 16/4/13.
 */
public class CheckSpecificOrderFraudDomain {
    public static class ClkMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> hourSet = new HashSet<String>();

        protected void setup(Context context) throws IOException,InterruptedException{
            for(int i=18;i<=23;i++) {
                this.hourSet.add(String.valueOf(i));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ClkMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ClkMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("ClkMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("ClkMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalClickLog.ClickLogMessage clkLog;
            try{
                clkLog = (OriginalClickLog.ClickLogMessage) TMC.parseFromString(value.toString(), OriginalClickLog.ClickLogMessage.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

//            if (!value.toString().contains("oid=24054")){
//                return;
//            }
            if (!value.toString().contains("cid=567")){
                return;
            }

            try {
                String ip = clkLog.getUserIp();
                String adx = AuxiliaryFunction.extractAdxId(clkLog.getData());
                String yoyiCookie = AuxiliaryFunction.extractYoyiCookie(clkLog.getData());
                String sid = AuxiliaryFunction.extractSessionId(clkLog.getData());
                String oid = AuxiliaryFunction.extractOrderId(clkLog.getData());
                String cid = AuxiliaryFunction.extractCampaignId(clkLog.getData());
                long timestamp = clkLog.getTimestamp();
                int hour;
                try {
                    hour = DateUtil.getTimeOfHour(timestamp);
                } catch (Exception e) {
                    return;
                }
                if(!this.hourSet.contains(String.valueOf(hour)))
                    return;

                key_result.set(sid+oid);
                value_result.set(String.valueOf(hour) +
                        Properties.Base.CTRL_A + "clkLog" +
                        Properties.Base.CTRL_A + adx +
                        Properties.Base.CTRL_A + yoyiCookie);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class ImpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> hourSet = new HashSet<String>();

        protected void setup(Context context) throws IOException,InterruptedException{
            for(int i=18;i<=23;i++) {
                this.hourSet.add(String.valueOf(i));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ImpMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("ImpMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("ImpMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("ImpMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalShowLog.ImpLogMessage impLog;
            try{
                impLog = (OriginalShowLog.ImpLogMessage) TMC.parseFromString(value.toString(), OriginalShowLog.ImpLogMessage.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

//            if (!value.toString().contains("oid=24054")){
//                return;
//            }
            if (!value.toString().contains("cid=567")){
                return;
            }

            try {
                String ip = impLog.getUserIp();
                String yoyiCookie = AuxiliaryFunction.extractYoyiCookie(impLog.getData());
                String adx = AuxiliaryFunction.extractAdxId(impLog.getData());
                String sid = AuxiliaryFunction.extractSessionId(impLog.getData());
                String oid = AuxiliaryFunction.extractOrderId(impLog.getData());
                String cid = AuxiliaryFunction.extractCampaignId(impLog.getData());
                long timestamp = impLog.getTimestamp();
                int hour;
                try {
                    hour = DateUtil.getTimeOfHour(timestamp);
                } catch (Exception e) {
                    return;
                }
                if(!this.hourSet.contains(String.valueOf(hour)))
                    return;

                key_result.set(sid+oid);
                value_result.set(String.valueOf(hour) +
                        Properties.Base.CTRL_A + "impLog" +
                        Properties.Base.CTRL_A + adx +
                        Properties.Base.CTRL_A + yoyiCookie);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class BidMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("BidMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("BidMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("BidMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("BidMapper", "otherExceptionCt");

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

            try {
                String ip = bidLog.getUser().getUserIp();
                String sid = bidLog.getSessionId();
                for (OriginalBidLog.Ad ad : bidLog.getAdsList()) {
                    String oid = ad.getOrderId();
                    String cid = ad.getCampaignId();
                    String domain = UrlUtil.getUrlDomain(bidLog.getPage().getPageUrl());
                    String adx = bidLog.getExchange().getAdxId();

//                    if (!oid.equals("24054"))
//                        return;
                    if (!cid.equals("567"))
                        return;

                    long timestamp = bidLog.getTimestamp();
                    int hour;
                    try {
                        hour = DateUtil.getTimeOfHour(timestamp);
                    } catch (Exception e) {
                        return;
                    }
                    key_result.set(sid+oid);
                    value_result.set(String.valueOf(hour) +
                            Properties.Base.CTRL_A + "bidLog" +
                            Properties.Base.CTRL_A + adx +
                            Properties.Base.CTRL_A + domain);
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            } catch(ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class CheckSpecificOrderFraudDomainReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckSpecificOrderFraudReducer", "validOutputCt");

            int clk = 0;
            int imp = 0;

            HashSet<String> domainSet = new HashSet<String>();
            String yoyiCookie = "";

            for (Text value : values) {
                String[] valueInfo = value.toString().split(Properties.Base.CTRL_A,-1);
                if (value.toString().contains("clkLog")){
                    clk ++;
                    yoyiCookie = valueInfo[3];
                } else if (value.toString().contains("impLog")) {
                    imp ++;
                    yoyiCookie = valueInfo[3];
                } else if (value.toString().contains("bidLog")) {
                    domainSet.add(valueInfo[3]);
                }
            }

            if (imp!=0) {
                String domainList = StringUtil.listToString(domainSet, Properties.Base.CTRL_B);
                value_result.set(domainList +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(imp) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(clk) +
                        Properties.Base.BS_SEPARATOR_TAB + yoyiCookie);
                context.write(NullWritable.get(), value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckSpecificOrderFraudDomain");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CheckSpecificOrderFraudDomain.class);
        job.setReducerClass(CheckSpecificOrderFraudDomainReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String clkLog = otherArgs[1];
        String impLog = otherArgs[2];
        String bidLog = otherArgs[3];

        CommUtil.addInputFileComm(job, fs, clkLog, TextInputFormat.class, ClkMapper.class);
        CommUtil.addInputFileComm(job, fs, impLog, TextInputFormat.class, ImpMapper.class);
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "18");
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "19");
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "20");
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "21");
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "22");
        CommUtil.addInputFileCommSucc(job, fs, bidLog, TextInputFormat.class, BidMapper.class, "23");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
