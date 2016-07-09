package com.mr.fraud_detection;

import com.mr.config.Properties;
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
import java.util.*;

/**
 * Created by TakiyaHideto on 16/3/25.
 */
public class CheckDomainKeepTime {
    public static class CheckDomainKeepTimeMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckAdzoneClickVarianceMapper", "validInput");
            Counter validOutputCt = context.getCounter("CheckAdzoneClickVarianceMapper", "validOutputCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);

            try {
                String url = elementInfo[5];
                String domain = UrlUtil.getUrlDomain(url);
                String keepTime = elementInfo[17];

                key_result.set(domain);
                value_result.set(keepTime);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class CheckDomainKeepTimeReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckAdzoneClickVarianceReducer", "validOutputCt");

            int keepTimeSum = 0;
            int reachCount = 0;
            for (Text value: values){
                try {
                    String valueString = value.toString();
                    int keepTime = Integer.parseInt(valueString);
                    keepTimeSum += keepTime;
                    reachCount++;
                } catch (NumberFormatException e){
                    continue;
                }
            }
            double keepTimeMean = (double)keepTimeSum/(double)reachCount;

            value_result.set(key.toString() +
                    Properties.Base.CTRL_A + String.valueOf(keepTimeSum) +
                    Properties.Base.CTRL_A + String.valueOf(reachCount) +
                    Properties.Base.CTRL_A + String.valueOf(keepTimeMean));
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

        Job job = new Job(conf,"CheckDomainKeepTime");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckDomainKeepTime.class);
        job.setReducerClass(CheckDomainKeepTimeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String dmpData = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<31;i++) {
            CommUtil.addInputFileComm(job, fs, dmpData + "/" + currentDate,
                    TextInputFormat.class, CheckDomainKeepTimeMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
