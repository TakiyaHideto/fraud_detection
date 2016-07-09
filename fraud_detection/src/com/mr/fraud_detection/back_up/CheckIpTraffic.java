package com.mr.fraud_detection.back_up;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalShowLog;
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
 * Created by hideto on 16/3/22.
 */
public class CheckIpTraffic {
    public static class CheckIpTrafficMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("CheckIpTrafficMapper", "validOutputCt");
            Counter validInputCt = context.getCounter("CheckIpTrafficMapper","validInputCt");
            Counter protoBufferExceptCt = context.getCounter("CheckIpTrafficMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckIpTrafficMapper","otherExceptionCt");

            TextMessageCodec TMC = new TextMessageCodec();
            OriginalShowLog.ImpLogMessage showLog;
            try{
                showLog = (OriginalShowLog.ImpLogMessage) TMC.parseFromString(value.toString(),OriginalShowLog.ImpLogMessage.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            String ip = showLog.getUserIp();
            key_result.set(ip);
            value_result.set("1");
            context.write(key_result,value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CheckIpTrafficReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckIpTrafficReducer", "validOutputCt");

            int impression = 0;

            for (Text value: values){
                impression++;
            }
            value_result.set(key.toString()+Properties.Base.CTRL_A+String.valueOf(impression));
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

        Job job = new Job(conf,"CheckIpTraffic");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckIpTraffic.class);
        job.setReducerClass(CheckIpTrafficReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String fraudBasedata = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, fraudBasedata, TextInputFormat.class, CheckIpTrafficMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
