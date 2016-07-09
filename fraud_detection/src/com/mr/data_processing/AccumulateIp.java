package com.mr.data_processing;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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
import java.util.List;

/**
 * Created by TakiyaHideto on 16/4/15.
 */
public class AccumulateIp {
    public static class AccumulateMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("AccumulateMapper","validOutputCt");
            Counter validInputCt = context.getCounter("AccumulateMapper","validInputCt");

            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);
            String device = elements[0];
            String dataType = elements[1];
            String deviceType = elements[2];
            String ipInfo = elements[3];

            key_result.set(device);
            value_result.set(dataType +
                    Properties.Base.CTRL_A + deviceType +
                    Properties.Base.CTRL_A + ipInfo);
            context.write(key_result,value_result);

        }
    }

    public static class AccumulateReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("AccumulateReducer", "validOutputCt");

            HashMap<String,HashSet<String>> ipTimeMap = new HashMap<String,HashSet<String>>();
            HashMap<String,Integer> timeFreqMap = new HashMap<String, Integer>();

            for (Text value: values){
                String valueString = value.toString();
                String[] elements = valueString.split(Properties.Base.CTRL_A,-1);
                String deviveType = elements[1];
                String[] ipList = elements[2].split(Properties.Base.CTRL_B,-1);
                for (String ipInfo: ipList){
                    String ip = ipInfo.split(Properties.Base.CTRL_C,-1)[0];
                    String timestamp = ipInfo.split(Properties.Base.CTRL_C,-1)[1];
                    String date = "";
                    String hour = "";
                    try{
                        date = String.valueOf(DateUtil.getTimeOfDate(Long.parseLong(timestamp)));
                        hour = String.valueOf(DateUtil.getTimeOfHour(Long.parseLong(timestamp)));
                    } catch (Exception e){
                        continue;
                    }
                    if (ipTimeMap.size()<500) {
                        CommUtil.addInfoToMapWithSet(ipTimeMap, ip, date + Properties.Base.CTRL_D + hour);
                        CommUtil.addInfoToMap(timeFreqMap, date + Properties.Base.CTRL_D + hour);
                    }
                }
            }
            String ipInfoString = "";
            for (String ip: ipTimeMap.keySet()){
                String timeString = "";
                for (String time: ipTimeMap.get(ip)){
                    timeString = time + Properties.Base.CTRL_D + String.valueOf(timeFreqMap.get(time));
                    ipInfoString += ip + Properties.Base.CTRL_C + timeString + Properties.Base.CTRL_B;
                }
            }
            ipInfoString = ipInfoString.substring(0,ipInfoString.length()-1);

            value_result.set(key.toString() + Properties.Base.CTRL_A + ipInfoString);
            context.write(NullWritable.get(), value_result);
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

        Job job = new Job(conf,"AccumulateIp");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(AccumulateIp.class);
        job.setReducerClass(AccumulateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(100);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLog = otherArgs[1];
        String currentData = otherArgs[2];

        for (int i=0;i<31;i++) {
            CommUtil.addInputFileComm(job, fs, bidLog + "/" + currentData + "/IP", TextInputFormat.class, AccumulateMapper.class);
            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
