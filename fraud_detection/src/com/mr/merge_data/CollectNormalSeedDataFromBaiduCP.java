package com.mr.merge_data;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.sun.org.apache.xpath.internal.operations.Bool;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/2/19.
 */
public class CollectNormalSeedDataFromBaiduCP {
    public static class CollectNormalSeedDataFromBaiduCPMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // create counters

            Counter validInputCt = context.getCounter("CollectNormalSeedDataFromBaiduCPMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("CollectNormalSeedDataFromBaiduCPMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] info = line.split(Properties.Base.BS_SEPARATOR, -1);
            String yoyiCookie = info[42];

            key_result.set(yoyiCookie);
            value_result.set(line);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class BaiduCpDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // create counters

            Counter validInputCt = context.getCounter("BaiduCpDataMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("BaiduCpDataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] info = line.split(Properties.Base.BS_SEPARATOR_TAB, -1);
            String yoyiCookie = info[1].split(Properties.Base.CTRL_B)[0];

            key_result.set(yoyiCookie);
            value_result.set("normal_data");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CollectNormalSeedDataFromBaiduCPReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CollectNormalSeedDataFromBaiduCPReducer", "validOutputCt");

            Boolean isBaiduCP = false;
            Boolean isDataTraffic = false;
            HashSet<String> dataSet = new HashSet<String>();

            for (Text value: values){
                String valueString = value.toString();
                if (!valueString.equals("normal_data")) {
                    dataSet.add(valueString);
                    isDataTraffic = true;
                }
                else
                    isBaiduCP = true;
            }

            if (isBaiduCP && isDataTraffic) {
                for (String data: dataSet) {
                    value_result.set(data);
                    context.write(NullWritable.get(), value_result);
                    validOutputCt.increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf, "CollectNormalSeedDataFromBaiduCP");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectNormalSeedDataFromBaiduCP.class);
		job.setReducerClass(CollectNormalSeedDataFromBaiduCPReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseDataFilePath = otherArgs[1];
        String normalDataFilePath = otherArgs[2];
        String currentDate = otherArgs[3];
        job.getConfiguration().set("currentDate",currentDate);
        job.getConfiguration().set("normalDataFilePath", normalDataFilePath);
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        CommUtil.addInputFileComm(job,fs,baseDataFilePath,TextInputFormat.class,CollectNormalSeedDataFromBaiduCPMapper.class);
        CommUtil.addInputFileComm(job, fs, normalDataFilePath, TextInputFormat.class, BaiduCpDataMapper.class, "");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
