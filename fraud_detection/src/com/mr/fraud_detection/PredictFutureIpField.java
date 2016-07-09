package com.mr.fraud_detection;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.TextMessageCodec;
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
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/4/13.
 */
public class PredictFutureIpField {
    public static class PredictFutureIpFieldClkMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("HadoopDemoMapper","validInputCt");
            Counter validOutputCt = context.getCounter("HadoopDemoMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("HadoopDemoMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("HadoopDemoMapper","otherExceptionCt");
            Counter cidExceptCt = context.getCounter("PredictFutureIpFieldClkMapper","cidExceptCt");
            Counter ipFieldExceptCt = context.getCounter("PredictFutureIpFieldClkMapper","ipFieldExceptCt");

            if (value.toString().contains("waicai")){
                return;
            }

            // initialization
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

            String ip;
            String ipField;
            try {
                ip = clickLog.getUserIp();
                ipField = extractIpField(ip);
            } catch (ArrayIndexOutOfBoundsException e){
                ipFieldExceptCt.increment(1);
                return;
            }
            long timestamp = clickLog.getTimestamp();
            int hour;
            try{
                hour = DateUtil.getTimeOfHour(timestamp);
            } catch (ParseException e){
                return;
            }

            try {
                String data = clickLog.getData();
                String campaignId = AuxiliaryFunction.extractCampaignId(data);

                key_result.set(campaignId +
                        Properties.Base.CTRL_A + String.valueOf(hour) +
                        Properties.Base.CTRL_A + ipField);
                value_result.set(ip + Properties.Base.CTRL_A + "clk");
                context.write(key_result, value_result);

                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                cidExceptCt.increment(1);
            }
        }

        public static String extractIpField(String ip){
            String[] temp = ip.split("\\.",-1);
            String ipField = temp[0] + "." + temp[1] + "." + temp[2];
            return ipField;
        }
    }

    public static class PredictFutureIpFieldImpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("HadoopDemoMapper","validInputCt");
            Counter validOutputCt = context.getCounter("HadoopDemoMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("HadoopDemoMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("HadoopDemoMapper","otherExceptionCt");
            Counter cidExceptCt = context.getCounter("PredictFutureIpFieldImpMapper","cidExceptCt");
            Counter ipFieldExceptCt = context.getCounter("PredictFutureIpFieldImpMapper","ipFieldExceptCt");

            if (value.toString().contains("waicai")){
                return;
            }

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalShowLog.ImpLogMessage showLog;
            try{
                showLog = (OriginalShowLog.ImpLogMessage) TMC.parseFromString(value.toString(), OriginalShowLog.ImpLogMessage.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            String ip;
            String ipField;
            try {
                ip = showLog.getUserIp();
                ipField = extractIpField(ip);
            } catch (ArrayIndexOutOfBoundsException e){
                ipFieldExceptCt.increment(1);
                return;
            }
            long timestamp = showLog.getTimestamp();
            int hour;
            try {
                hour = DateUtil.getTimeOfHour(timestamp);
            } catch (ParseException e) {
                return;
            }
            String data = showLog.getData();
            try {
                String campaignId = AuxiliaryFunction.extractCampaignId(data);


                key_result.set(campaignId +
                        Properties.Base.CTRL_A + String.valueOf(hour) +
                        Properties.Base.CTRL_A + ipField);
                value_result.set(ip + Properties.Base.CTRL_A + "imp");
                context.write(key_result, value_result);

                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e) {
                cidExceptCt.increment(1);
            }
        }

        public static String extractIpField(String ip){
            String[] temp = ip.split("\\.",-1);
            String ipField = temp[0] + "." + temp[1] + "." + temp[2];
            return ipField;
        }
    }

    public static class PredictFutureFrdIpReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            int clk = 0;
            int imp = 0;

            HashSet<String> ipSet = new HashSet<String>();

            for (Text value: values){
                String[] valueElements = value.toString().split(Properties.Base.CTRL_A,-1);
                if (value.toString().contains("clk")){
                    clk++;
                    imp++;
                } else if (value.toString().contains("imp")){
                    imp++;
                }
                ipSet.add(valueElements[0]);
            }

            double ctr = (double)clk/(double)(imp);

            if ((ctr>0.03 && clk>10) || (ctr>0.05 && clk>7)){
                for (String ip: ipSet) {
                    value_result.set(ip);
                    context.write(NullWritable.get(), value_result);
                }
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

        Job job = new Job(conf,"PredictFutureFrdIp");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(PredictFutureFrdIp.class);
        job.setReducerClass(PredictFutureFrdIpReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String originalClkPath = otherArgs[1];
        String originalImpPath = otherArgs[2];

        CommUtil.addInputFileComm(job, fs, originalClkPath, TextInputFormat.class, PredictFutureIpFieldClkMapper.class);
        CommUtil.addInputFileComm(job, fs, originalImpPath, TextInputFormat.class, PredictFutureIpFieldImpMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
