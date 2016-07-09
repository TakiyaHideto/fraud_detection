package com.mr.fraud_detection.back_up;

import com.mr.config.Properties;
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
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by hideto on 16/3/6.
 */
public class TestRobotFraudValidityPart0 {
    public static class TestRobotFraudValidityMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException {
            // create counters
            Counter validInputCt = context.getCounter("TestRobotFraudValidityMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("TestRobotFraudValidityMapper", "validOutputCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A, -1);
            String clk = elementInfo[2];
            String domain = elementInfo[22];
            String adzoneId = elementInfo[27];
            String logDate = elementInfo[31];
            String userAgent = elementInfo[35];
            String browser = elementInfo[36];
            String os = elementInfo[37];
            String ip = elementInfo[40];
            String yoyiCookie = elementInfo[42];

            key_result.set(ip);
            value_result.set(clk +
                    Properties.Base.CTRL_C + domain +
                    Properties.Base.CTRL_C + adzoneId +
                    Properties.Base.CTRL_C + logDate +
                    Properties.Base.CTRL_C + userAgent +
                    Properties.Base.CTRL_C + browser +
                    Properties.Base.CTRL_C + os +
                    Properties.Base.CTRL_C + ip +
                    Properties.Base.CTRL_C + yoyiCookie);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class TestRobotFraudValidityReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("TestRobotFraudValidityReducer", "validOutputCt");

            String clk = "";
            String domain = "";
            String adzoneId = "";
            String logDate = "";
            String userAgent = "";
            String browser = "";
            String os = "";
            String ip = "";
            String yoyiCookie = "";
            String ipField = "";

            String infoString = "";

            String randomMockingIpField = "";
            for (int i=0;i<17;i++){
                if (Math.random()<0.5){
                    randomMockingIpField += "A";
                } else {
                    randomMockingIpField += "B";
                }
            }

            for (Text value: values){
                String valueString = value.toString();
                infoString += valueString + Properties.Base.CTRL_B;
            }
            infoString = infoString.substring(0,infoString.length()-1);
            value_result.set(key.toString() +
                    Properties.Base.CTRL_A + randomMockingIpField +
                    Properties.Base.CTRL_A + infoString);
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
        }

        public static void addInfoToMapInteger(HashMap<String,Integer> map, String keyInfo){
            if (map.containsKey(keyInfo)){
                int count = map.get(keyInfo);
                map.remove(keyInfo);
                map.put(keyInfo,count+1);
            } else {
                map.put(keyInfo,1);
            }
        }

        public static void addInfoToMapSet(HashMap<String,HashSet<String>> map, String keyInfo, String valueInfo){
            HashSet<String> set = new HashSet<String>();
            if (map.containsKey(keyInfo)){
                set = map.get(keyInfo);
                set.add(valueInfo);
                map.remove(keyInfo);
                map.put(keyInfo,set);
            } else {
                set.add(valueInfo);
                map.put(keyInfo,set);
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

        Job job = new Job(conf,"TestRobotFraudValidityPart0");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(TestRobotFraudValidityPart0.class);
        job.setReducerClass(TestRobotFraudValidityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(70);
        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedata = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<1;i++) {
            CommUtil.addInputFileComm(job, fs, basedata + "/log_date=" + currentDate, TextInputFormat.class, TestRobotFraudValidityMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
