package com.mr.fraud_detection;

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
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/3/15.
 */
public class CheckIpBounceRate {
    public static class CheckIpBounceRateMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("CheckIpBounceRateMapper", "validOutputCt");

            try {

                String line = value.toString();
                String[] elements = line.split(Properties.Base.CTRL_A, -1);

                String ip = elements[13];
                String period = elements[17];
                String timestamp = elements[0];
                String date = "";
                try {
                    date = DateUtil.getTimeOfDate(Long.parseLong(timestamp));
                } catch (ParseException e) {
                    return;
                }

                key_result.set(ip + Properties.Base.CTRL_A + date);
                value_result.set(period);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (Exception e){
                return;
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

            int sumPeriod = 0;
            int count = 0;
            int noneBounceSumPeriod = 0;
            int noneBounceCount = 0;

            for (Text value: values){
                try {
                    String valueString = value.toString();
                    sumPeriod += Integer.parseInt(valueString);
                    count++;
                    if (!valueString.equals("0")) {
                        noneBounceSumPeriod += Integer.parseInt(valueString);
                        noneBounceCount++;
                    }
                } catch (Exception e ){
                    continue;
                }
            }

            double meanPeriod = (double)sumPeriod/(double)count;
            double noneBounceMeanPeriod = (double)noneBounceSumPeriod/(double)noneBounceCount;

            value_result.set(key.toString() +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sumPeriod) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(count) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(meanPeriod) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(noneBounceSumPeriod) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(noneBounceCount) +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(noneBounceMeanPeriod));
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

        Job job = new Job(conf,"CheckIpBounceRate");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckIpBounceRate.class);
        job.setReducerClass(CheckFraudDetectionBounceRateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String fraudBasedata = otherArgs[1];
        String currentData = otherArgs[2];

        for (int i=0;i<31;i++) {
            CommUtil.addInputFileComm(job, fs, fraudBasedata+"/"+currentData, TextInputFormat.class, CheckIpBounceRateMapper.class);
            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
