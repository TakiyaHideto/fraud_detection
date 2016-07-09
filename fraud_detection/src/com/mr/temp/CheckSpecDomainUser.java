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

/**
 * Created by TakiyaHideto on 16/4/11.
 */
public class CheckSpecDomainUser {
    public static class CheckSpecDomainUserMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

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

            if (!value.toString().contains("oid=21884")){
                return;
            }
            if (!value.toString().contains("cid=731")){
                return;
            }

            try {
                String ip = clkLog.getUserIp();
                String adx = AuxiliaryFunction.extractAdxId(clkLog.getData());
                String oid = AuxiliaryFunction.extractOrderId(clkLog.getData());
                String cid = AuxiliaryFunction.extractCampaignId(clkLog.getData());
                String sid = AuxiliaryFunction.extractSessionId(clkLog.getData());
                long timestamp = clkLog.getTimestamp();
                int hour;
                try {
                    hour = DateUtil.getTimeOfHour(timestamp);
                } catch (Exception e) {
                    return;
                }

                if (hour==17||hour==18||hour==19) {
                    key_result.set(sid +
                            Properties.Base.BS_SEPARATOR_UNDERLINE + oid);
                    value_result.set(ip +
                            Properties.Base.CTRL_A + String.valueOf(hour) +
                            Properties.Base.CTRL_A + "clkLog" +
                            Properties.Base.CTRL_A + adx);
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }


    public static class CheckSpecDomainUserReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckSpecificOrderFraudReducer", "validOutputCt");

            int clk = 0;
            int imp = 0;
            int bid = 0;

            HashMap<String,Integer> adxMap = new HashMap<String,Integer>();
            HashMap<String,Integer> domainMap = new HashMap<String,Integer>();

            for (Text value : values) {
                String[] valueInfo = value.toString().split(Properties.Base.CTRL_A,-1);
                if (valueInfo[2].equals("clkLog")){
                    clk ++;
                }
                if (valueInfo[2].equals("impLog")) {
                    imp ++;
                }
                if (valueInfo[2].equals("bidLog")){
                    bid ++;
                    this.addInfoToMap(domainMap, valueInfo[4]);
                }
            }

            String domainList = StringUtil.mapToString(domainMap, Properties.Base.CTRL_B);
            value_result.set(domainList +
                    Properties.Base.CTRL_A + String.valueOf(bid) +
                    Properties.Base.CTRL_A + String.valueOf(imp) +
                    Properties.Base.CTRL_A + String.valueOf(clk));
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);

        }

        public static void addInfoToMap(HashMap<String,Integer> map, String keyInfo){
            if (map.containsKey(keyInfo)){
                int count = map.get(keyInfo);
                map.remove(keyInfo);
                map.put(keyInfo,count+1);
            } else {
                map.put(keyInfo,1);
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

        Job job = new Job(conf,"CheckSpecDomainUser");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CheckSpecDomainUser.class);
        job.setReducerClass(CheckSpecDomainUserReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String clkLog = otherArgs[1];
        String impLog = otherArgs[2];
        String bidLog = otherArgs[3];

        CommUtil.addInputFileComm(job, fs, clkLog, TextInputFormat.class, CheckSpecDomainUserMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
