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
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/4/8.
 */
public class YoyiCpPart3ExcludeCp {
    public static class YoyiCpPart3ExcludeCpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();

            key_result.set(line);
            value_result.set("candidate");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart3CpDeviceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CrossPlatformBasicJoiningSingleDayMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String[] deviceInfo = line.split(Properties.Base.CTRL_A,-1);

            key_result.set(deviceInfo[1] + Properties.Base.CTRL_A + deviceInfo[0]);
            value_result.set("cpDevice");
            context.write(key_result,value_result);
            validOutputCt.increment(1);

        }
    }

    public static class YoyiCpPart3ExcludeCpReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, java.lang.Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validInputCt = context.getCounter("YoyiCpPart3ExcludeCpReducer", "validInputCt");
            Counter validOutputCt = context.getCounter("YoyiCpPart3ExcludeCpReducer", "validOutputCt");

            validInputCt.increment(1);

            for (Text value: values){
                if (value.toString().equals("cpDevice")){
                    return;
                }
            }

            context.write(NullWritable.get(), key);
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

        Job job = new Job(conf,"YoyiCpPart3ExcludeCp");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

//        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
//        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
//        job.getConfiguration().setBoolean("mapred.output.compress", true);
//        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart3ExcludeCp.class);
        job.setReducerClass(YoyiCpPart3ExcludeCpReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        MultipleOutputs.addNamedOutput(job,"idfa",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"imei",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"adroidId",TextOutputFormat.class,NullWritable.class, Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String candidateFile = otherArgs[1];
        String cpDeviceFile = otherArgs[2];

        CommUtil.addInputFileComm(job, fs, candidateFile, TextInputFormat.class, YoyiCpPart3ExcludeCpMapper.class);
        CommUtil.addInputFileComm(job, fs, cpDeviceFile, TextInputFormat.class, YoyiCpPart3CpDeviceMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
