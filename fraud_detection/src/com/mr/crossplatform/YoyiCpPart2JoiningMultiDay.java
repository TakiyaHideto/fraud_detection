package com.mr.crossplatform;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.lang.Exception;import java.lang.InterruptedException;import java.lang.Object;import java.lang.Override;import java.lang.String;import java.lang.System;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/2/24.
 */
public class YoyiCpPart2JoiningMultiDay {
    public static class CrossPlatformBasicJoiningMultiDayMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> idfaSet = new HashSet<String>();
        private HashSet<String> imeiSet = new HashSet<String>();
        private FileSystem fs = null;

//        protected void setup(Context context) throws IOException,InterruptedException{
//            Configuration conf = context.getConfiguration();
//            String currentdate = context.getConfiguration().get("currentDate");
//            String mobileDeviceDataPath = context.getConfiguration().get("mobileDevicePath");
//            fs = FileSystem.get(conf);
//            loadCache(fs, mobileDeviceDataPath, this.idfaSet);
//        }
//
//        public static void loadCache(FileSystem fs, String path, HashSet<String> set)
//                throws FileNotFoundException, IOException{
//            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
//            String line;
//            while((line = br.readLine())!=null){
//                String device = line.trim();
//                if (!device.equals(""))
//                    set.add(device);
//            }
//            br.close();
//        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);

            HashSet<String> cookieCpSet = new HashSet<String>();
            HashSet<String> idfaCpSet = new HashSet<String>();
            HashSet<String> imeiCpSet = new HashSet<String>();
            HashSet<String> androidCpSet = new HashSet<String>();
            String cookieList = "";
            String idfaList = "";
            String imeiList = "";
            String androidList = "";

            String ip = elementInfo[0];
            String date = elementInfo[1];
            String[] deviceList = elementInfo[2].split(Properties.Base.CTRL_B,-1);

            validInputCt.increment(1);

            for (String device: deviceList){
                if (device.startsWith("1")){
                    cookieCpSet.add(device.substring(2,device.length()));
                } else if (device.startsWith("2")){
                    idfaCpSet.add(device);
                } else if (device.startsWith("3")){
                    imeiCpSet.add(device);
                } else if (device.startsWith("4")){
                    androidCpSet.add(device);
                }
            }
            cookieList = StringUtil.listToString(cookieCpSet,Properties.Base.CTRL_B);
//            idfaList = StringUtil.listToString(idfaCpSet,Properties.Base.CTRL_B);
//            imeiList = StringUtil.listToString(imeiCpSet,Properties.Base.CTRL_B);
//            androidList = StringUtil.listToString(androidCpSet,Properties.Base.CTRL_B);

            for (String idfa: idfaCpSet){
                    key_result.set(idfa);
                    value_result.set(date +
                            Properties.Base.CTRL_A + cookieList);
                    context.write(key_result,value_result);
                    validOutputCt.increment(1);
            }

            for (String imei: imeiCpSet){
                    key_result.set(imei);
                    value_result.set(date +
                            Properties.Base.CTRL_A + cookieList);
                    context.write(key_result,value_result);
                    validOutputCt.increment(1);
            }

            for (String androidId: androidCpSet){
                key_result.set(androidId);
                value_result.set(date +
                        Properties.Base.CTRL_A + cookieList);
                context.write(key_result,value_result);
                validOutputCt.increment(1);
            }

            cookieCpSet.clear();
            idfaCpSet.clear();
            imeiCpSet.clear();
        }
    }

    public static class CrossPlatformBasicJoiningMultiDayReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private int activeDayThreshold = 1;
        private MultipleOutputs<NullWritable, Text> mos;

        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable,Text>(context);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        // 统计频次
        protected void addDeviceInfo(HashMap<String,HashSet<String>> deviceInfoMap, String deviceInfo, String date){
            if (!deviceInfo.equals("")) {
                if (deviceInfoMap.containsKey(deviceInfo)) {
                    HashSet<String> dateSet = deviceInfoMap.get(deviceInfo);
                    dateSet.add(date);
                    deviceInfoMap.remove(deviceInfo);
                    deviceInfoMap.put(deviceInfo,dateSet);
                } else {
                    HashSet<String> dateSet = new HashSet<String>();
                    dateSet.add(date);
                    deviceInfoMap.put(deviceInfo,dateSet);
                }
            }
        }

        protected String mapInfoToString(HashMap<String,HashSet<String>> map, Counter ct){
            String deviceString = "";
            for (String key: map.keySet()){
                if (map.get(key).size()>this.activeDayThreshold){
                    deviceString += key +
//                            Properties.Base.CTRL_C + String.valueOf(map.get(key).size()) +
                            Properties.Base.CTRL_B;
                    ct.increment(1);
                }
            }
            return deviceString;
        }

        @Override
        protected void reduce(Text key, java.lang.Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningMultiDayReducer", "validOutputCt");
            Counter mobileDeviceCt = context.getCounter("CrossPlatformBasicJoiningMultiDayReducer","mobileDeviceCt");
            Counter cookieCt = context.getCounter("CrossPlatformBasicJoiningMultiDayReducer","cookieCt");

            HashMap<String,HashSet<String>> deviceCookieMap = new HashMap<String,HashSet<String>>();

            HashSet<String> deviceSet = new HashSet<String>();

            for (Text value: values){
                String[] valueString = value.toString().split(Properties.Base.CTRL_A,-1);
                String date = valueString[0];
                String cookieCluster = valueString[1];
                for (String cookie: cookieCluster.split(Properties.Base.CTRL_B,-1)) {
                    try {
                        this.addDeviceInfo(deviceCookieMap,cookie,date);
                    } catch (StringIndexOutOfBoundsException e){
                        return;
                    }
                }
            }

            for (String cookie: deviceCookieMap.keySet()) {
                if (deviceCookieMap.get(cookie).size()>2) {
                    value_result.set(key.toString().substring(2,key.toString().length()) + Properties.Base.CTRL_A + cookie);
                    context.write(NullWritable.get(),value_result);
                    validOutputCt.increment(1);
                }
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
        if(otherArgs.length != 3){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"YoyiCpPart2JoiningMultiDay");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart2JoiningMultiDay.class);
        job.setReducerClass(CrossPlatformBasicJoiningMultiDayReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        MultipleOutputs.addNamedOutput(job,"idfa",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"imei",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"adroidId",TextOutputFormat.class,NullWritable.class, Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidlogPath = otherArgs[1];
        String currentDate = otherArgs[2];
        job.getConfiguration().set("currentDate",currentDate);

        for (int i=0;i<30;i++) {
            CommUtil.addInputFileComm(job,fs,bidlogPath+currentDate,
                    TextInputFormat.class,CrossPlatformBasicJoiningMultiDayMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

//        CommUtil.addInputFileComm(job,fs,specificCookie,
//                TextInputFormat.class,SpecificCookieMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
