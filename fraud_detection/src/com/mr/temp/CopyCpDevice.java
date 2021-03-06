package com.mr.temp;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
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

import java.io.IOException;

/**
 * Created by TakiyaHideto on 16/5/9.
 */
public class CopyCpDevice {
    public static class CopyCpDeviceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();


        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CopyCpDevice","validInputCt");
            Counter validOutputCt = context.getCounter("CopyCpDevice", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("CopyCpDevice", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CopyCpDevice", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);
            String cookie = elements[0];
            String mobile = elements[1];
            String prob = elements[2];

            key_result.set(cookie + Properties.Base.CTRL_A + mobile);
            value_result.set(prob);
            context.write(key_result, value_result);
        }
    }


    public static class CopyCpDeviceReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CopyCpDevice", "validOutputCt");

            String prob = "";
            for (Text value: values){
                prob = value.toString();
            }
            value_result.set(key.toString() + Properties.Base.CTRL_A + prob);
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CopyCpDevice");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CheckOidFrdUser.class);
        job.setReducerClass(CopyCpDeviceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(100);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String clkLog = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, clkLog, TextInputFormat.class, CopyCpDeviceMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
