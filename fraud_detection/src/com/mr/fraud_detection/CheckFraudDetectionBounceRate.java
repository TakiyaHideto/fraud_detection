package com.mr.fraud_detection;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.StringUtil;
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
import sun.awt.image.ImageWatched;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/2/25.
 */
public class CheckFraudDetectionBounceRate {
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
                String duration = elementInfo[17];

                key_result.set(yoyiCookie);
                value_result.set(duration + Properties.Base.CTRL_A + "monitor");
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                context.getCounter("MonitorDataMapper","arrayIndexOutOfBoundsExceptionCt").increment(1);
            }
        }
    }

    public static class PredictionDataMapper extends Mapper<Object, Text, Text, Text> {
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
            String fraudProbability = elementInfo[7].split(Properties.Base.CTRL_B,-1)[0];
            if (!fraudProbability.equals("")) {
                String fraudTag;
                double threFrd = Double.parseDouble(context.getConfiguration().get("thresholdFrd"));
                double threNml = Double.parseDouble(context.getConfiguration().get("thresholdNml"));
                if (Double.parseDouble(fraudProbability)>threFrd){
                    fraudTag = "1";
                } else if (Double.parseDouble(fraudProbability)<threNml){
                    fraudTag = "0";
                } else {
                    fraudTag = "-";
                }
                key_result.set(yoyiCookie);
                value_result.set(fraudTag + Properties.Base.CTRL_A + "basedata");
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            }
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
            Boolean isBasedata = false;
            Boolean isFraud;

            LinkedList<String> durationList = new LinkedList<String>();
            HashMap<String,Integer> fraudTagMap = new HashMap<String,Integer>();

            for (Text value: values){
                String[] valueString = value.toString().split(Properties.Base.CTRL_A,-1);
                if (valueString[1].contains("basedata")){
                    isBasedata = true;
                    addInfoToMap(fraudTagMap,valueString[0]);
                } else if (valueString[1].contains("monitor")){
                    isMonitor = true;
                    durationList.add(valueString[0]);
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
                isFraud = true;
            }

            String durationString = StringUtil.listToString(durationList,Properties.Base.BS_SEPARATOR_TAB);

            if (isMonitor && isBasedata){
                if (isFraud){
                    value_result.set(key.toString() +
                            Properties.Base.CTRL_A + "1" +
                            Properties.Base.CTRL_A + durationString);
                    context.write(NullWritable.get(), value_result);
                } else {
                    value_result.set(key.toString() +
                            Properties.Base.CTRL_A + "0" +
                            Properties.Base.CTRL_A + durationString);
                    context.write(NullWritable.get(), value_result);
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
        if(otherArgs.length != 5){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckFraudDetectionBounceRate");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckFraudDetectionBounceRate.class);
        job.setReducerClass(CheckFraudDetectionBounceRateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedata = otherArgs[1];
        String monitorData = otherArgs[2];
        String thresholdFrd = otherArgs[3];
        String thresholdNml = otherArgs[4];
        job.getConfiguration().set("thresholdFrd",thresholdFrd);
        job.getConfiguration().set("thresholdNml",thresholdNml);

        CommUtil.addInputFileComm(job, fs, monitorData, TextInputFormat.class, MonitorDataMapper.class);
        CommUtil.addInputFileComm(job, fs, basedata, TextInputFormat.class, PredictionDataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
