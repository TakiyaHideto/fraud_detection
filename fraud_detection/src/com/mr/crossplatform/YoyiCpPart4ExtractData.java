package com.mr.crossplatform;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by TakiyaHideto on 16/4/8.
 */
public class YoyiCpPart4ExtractData {
    public static class YoyiCpPart4DeviceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String mobile = line.split(Properties.Base.CTRL_A,-1)[0];
            String cookie = line.split(Properties.Base.CTRL_A,-1)[1];

            key_result.set(mobile);
            value_result.set("CANDIDATE");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

            key_result.set(cookie);
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataIpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String device = line.split(Properties.Base.CTRL_A,-1)[0];

            key_result.set(device);
            value_result.set(line + Properties.Base.CTRL_A + "IP_LOG");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataDomainMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String device = line.split(Properties.Base.CTRL_A,-1)[0];

            key_result.set(device);
            value_result.set(line + Properties.Base.CTRL_A + "DOMAIN_LOG");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataCrowdTagMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String device = line.split(Properties.Base.CTRL_A,-1)[0];

            key_result.set(device);
            value_result.set(line + Properties.Base.CTRL_A + "CROWDTAG_LOG");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataAreaCodeMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String device = line.split(Properties.Base.CTRL_A,-1)[0];

            key_result.set(device);
            value_result.set(line + Properties.Base.CTRL_A + "AREACODE_LOG");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataAppTypeMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String device = line.split(Properties.Base.CTRL_A,-1)[0];

            key_result.set(device);
            value_result.set(line + Properties.Base.CTRL_A + "APPTYPE_LOG");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart4ExtractDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, java.lang.Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validInputCt = context.getCounter("YoyiCpPart4ExtractDataReducer", "validInputCt");
            Counter validOutputCt = context.getCounter("YoyiCpPart4ExtractDataReducer", "validOutputCt");

            ArrayList<String> dataList = new ArrayList<String>();

            validInputCt.increment(1);

            Boolean flag = false;

            for (Text value: values){
                if (value.toString().equals("CANDIDATE")){
                    flag = true;
                } else {
                    dataList.add(value.toString());
                }
            }

            if (flag) {
                for (String data : dataList) {
                    value_result.set(data);
                    context.write(NullWritable.get(), value_result);
                    validOutputCt.increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 8){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"YoyiCpPart4ExtractData");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart4ExtractData.class);
        job.setReducerClass(YoyiCpPart4ExtractDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(300);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String ipFile = otherArgs[1];
        String domainFile = otherArgs[2];
        String crowdTagFile = otherArgs[3];
        String areaCodeFile = otherArgs[4];
        String appTypeFile = otherArgs[5];
        String candidateFile = otherArgs[6];
        String currentData = otherArgs[7];

        CommUtil.addInputFileComm(job, fs, ipFile, TextInputFormat.class, YoyiCpPart4ExtractDataIpMapper.class);
        CommUtil.addInputFileComm(job, fs, domainFile, TextInputFormat.class, YoyiCpPart4ExtractDataDomainMapper.class);
        CommUtil.addInputFileComm(job, fs, crowdTagFile, TextInputFormat.class, YoyiCpPart4ExtractDataCrowdTagMapper.class);
        CommUtil.addInputFileComm(job, fs, areaCodeFile, TextInputFormat.class, YoyiCpPart4ExtractDataAreaCodeMapper.class);
        CommUtil.addInputFileComm(job, fs, appTypeFile, TextInputFormat.class, YoyiCpPart4ExtractDataAppTypeMapper.class);
        CommUtil.addInputFileComm(job, fs, candidateFile, TextInputFormat.class, YoyiCpPart4DeviceMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
