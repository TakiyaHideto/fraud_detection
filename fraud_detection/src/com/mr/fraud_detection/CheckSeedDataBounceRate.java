package com.mr.fraud_detection;

import com.mr.code_backup.DataHandler;
import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.StringUtil;
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
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/3/3.
 */
public class CheckSeedDataBounceRate {
    public static class MonitorDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("MonitorDataMapper", "validOutputCt");

            // initialization
            try {
                String line = value.toString();
                String[] elementInfo = line.split(Properties.Base.CTRL_A, -1);
                String yoyiCookie = elementInfo[1];
                String ip = elementInfo[13];
                String duration = elementInfo[17];

                key_result.set(ip);
                value_result.set(duration + Properties.Base.CTRL_A + "monitor");
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                context.getCounter("MonitorDataMapper","arrayIndexOutOfBoundsExceptionCt").increment(1);
            }
        }
    }

    public static class FraudDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("PredictionDataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String yoyiCookie = elementInfo[42];
            String ip = elementInfo[40];
            key_result.set(ip);
            value_result.set("1" + Properties.Base.CTRL_A +"basedata_fraud");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class NormalDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("PredictionDataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String yoyiCookie = elementInfo[42];
            String ip = elementInfo[40];
            key_result.set(ip);
            value_result.set("0" + Properties.Base.CTRL_A + "basedata_normal");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CheckFraudDetectionBounceRateReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckFraudDetectionBounceRateReducer", "validOutputCt");

            HashMap<String,String> timeUrlMap = new HashMap<String,String>();
            Boolean isMonitor = false;
            Boolean isBasedataFraud = false;
            Boolean isBasedataNormal = false;
            Boolean isFraud;
            int durationCount = 0;

            LinkedList<String> durationList = new LinkedList<String>();
            HashMap<String,Integer> fraudTagMap = new HashMap<String,Integer>();

            for (Text value: values){
                String[] valueString = value.toString().split(Properties.Base.CTRL_A,-1);
                if (valueString[1].contains("basedata_normal")){
                    isBasedataNormal = true;
                    addInfoToMap(fraudTagMap,valueString[0]);
                } else if (valueString[1].contains("basedata_fraud")){
                    isBasedataFraud = true;
                    addInfoToMap(fraudTagMap,valueString[0]);
                } else if (valueString[1].contains("monitor")){
                    isMonitor = true;
                    durationList.add(valueString[0]);
                    durationCount++;
                }
            }

            if(fraudTagMap.containsKey("0") && fraudTagMap.containsKey("1")) {
                if (fraudTagMap.get("0") > fraudTagMap.get("1")) {
                    isFraud = false;
                } else if (fraudTagMap.get("0") < fraudTagMap.get("1")) {
                    isFraud = true;
                } else {
                    if (Math.random() <= 0.5) {
                        isFraud = true;
                    } else {
                        isFraud = false;
                    }
                }
            } else if (fraudTagMap.containsKey("0")){
                isFraud = false;
            } else if (fraudTagMap.containsKey("1")){
                isFraud = true;
            } else {
                if (Math.random()>0.5)
                    isFraud = false;
                else
                    isFraud = true;
            }

            String durationString = StringUtil.listToString(durationList, Properties.Base.BS_SEPARATOR_TAB);

            if ((isMonitor&&isBasedataNormal)||(isMonitor&&isBasedataFraud)){
                if (isFraud){
                    value_result.set(key.toString() +
                            Properties.Base.CTRL_A + "1" +
                            Properties.Base.CTRL_A + durationString +
                            Properties.Base.CTRL_A + String.valueOf(durationCount));
                    context.write(NullWritable.get(), value_result);
                    context.getCounter("CheckFraudDetectionBounceRateReducer","fraud").increment(1);
                } else {
                    value_result.set(key.toString() +
                            Properties.Base.CTRL_A + "0" +
                            Properties.Base.CTRL_A + durationString +
                            Properties.Base.CTRL_A + String.valueOf(durationCount));
                    context.write(NullWritable.get(), value_result);
                    context.getCounter("CheckFraudDetectionBounceRateReducer","normal").increment(1);
                }
                validOutputCt.increment(1);
            }
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
        if(otherArgs.length != 6){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckSeedDataBounceRate");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckSeedDataBounceRate.class);
        job.setReducerClass(CheckFraudDetectionBounceRateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String fraudBasedata = otherArgs[1];
        String normalBasedata = otherArgs[2];
        String normalBaiduBasedata = otherArgs[3];
        String monitorData = otherArgs[4];
        String currentDate = otherArgs[5];

//        for (int i=0;i<7;i++) {
//            CommUtil.addInputFileComm(job, fs, monitorData+"/log_date="+currentDate, TextInputFormat.class, MonitorDataMapper.class);
//            CommUtil.addInputFileComm(job, fs, fraudBasedata+"/log_date="+currentDate, TextInputFormat.class, FraudDataMapper.class);
//            CommUtil.addInputFileComm(job, fs, normalBasedata+"/log_date="+currentDate, TextInputFormat.class, NormalDataMapper.class);
//            CommUtil.addInputFileComm(job, fs, normalBaiduBasedata+"/log_date="+currentDate, TextInputFormat.class, NormalDataMapper.class);
//            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
//        }

        CommUtil.addInputFileComm(job, fs, monitorData, TextInputFormat.class, MonitorDataMapper.class);
        CommUtil.addInputFileComm(job, fs, fraudBasedata, TextInputFormat.class, FraudDataMapper.class);
        CommUtil.addInputFileComm(job, fs, normalBasedata, TextInputFormat.class, NormalDataMapper.class);
        CommUtil.addInputFileComm(job, fs, normalBaiduBasedata, TextInputFormat.class, NormalDataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
