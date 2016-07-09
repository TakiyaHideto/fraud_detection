package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
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
import java.lang.Exception;
import java.lang.InterruptedException;
import java.lang.Iterable;
import java.lang.Object;
import java.lang.String;
import java.lang.System;
import java.util.HashSet;
import java.util.HashMap;

/**
 * Created by TakiyaHideto on 16/2/24.
 */
public class YoyiCpPart1JoiningSingleDay {
    public static class CrossPlatformBasicJoiningSingleDayMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String device = elementInfo[0];
            String deviceType = elementInfo[2];
            String[] ipOccupation = elementInfo[3].split(Properties.Base.CTRL_B,-1);

            validInputCt.increment(1);
//            if (deviceType.equals("4"))
//                return;

            // output key-value
            for (String ipAct: ipOccupation) {
                String ip = ipAct.split(Properties.Base.CTRL_C,-1)[0];
                key_result.set(ip);
                value_result.set(deviceType+":"+device);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static class CrossPlatformBasicJoiningSingleDayReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        // 统计频次
        protected int addDeviceInfo(HashSet<String> set, String device){
            set.add(device);
            return set.size();
        }

        protected void deleteDeviceInfo(HashMap<String,java.lang.Integer> deviceInfoMap, String deviceInfo){
            deviceInfoMap.remove(deviceInfo);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayReducer", "validOutputCt");

            HashSet<String> cookieSet = new HashSet<String>();
            HashSet<String> mobileSet = new HashSet<String>();

            for (Text value: values){
                String valueString = value.toString();
                if (valueString.startsWith("1")){
                    int deviceNum = this.addDeviceInfo(cookieSet,valueString);
                    if(deviceNum>500){
                        return;
                    }
                } else if (valueString.startsWith("2") || valueString.startsWith("3") || valueString.startsWith("4")){
                    int deviceNum = this.addDeviceInfo(mobileSet,valueString);
                    if(deviceNum>500){
                        return;
                    }
                }
            }

            String deviceList = "";
            for (String cookie: cookieSet){
                deviceList += cookie + Properties.Base.CTRL_B;
            }
            for (String mobile: mobileSet){
                deviceList += mobile + Properties.Base.CTRL_B;
            }

            value_result.set(key.toString() +
                    Properties.Base.CTRL_A + context.getConfiguration().get("currentDate") +
                    Properties.Base.CTRL_A + deviceList.substring(0,deviceList.length()-1));
            context.write(NullWritable.get(),value_result);
            validOutputCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"YoyiCpPart1JoiningSingleDay");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        job.getConfiguration().set("mapreduce.map.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        job.getConfiguration().set("mapreduce.reduce.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart1JoiningSingleDay.class);
        job.setReducerClass(CrossPlatformBasicJoiningSingleDayReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidlogPath = otherArgs[1];
        String currentDate = otherArgs[2];
        job.getConfiguration().set("currentDate",currentDate);

        CommUtil.addInputFileComm(job, fs, bidlogPath, TextInputFormat.class, CrossPlatformBasicJoiningSingleDayMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
