package com.mr.temp;

import com.mr.protobuffer.OriginalBidLog;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/3/25.
 */
public class ExtractLingJiCookie {
    public static class ExtractLingJiCookieMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("HadoopDemoMapper","validInputCt");
            Counter validOutputCt = context.getCounter("HadoopDemoMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("HadoopDemoMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("HadoopDemoMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            String adx = bidLog.getExchange().getAdxId();
            if(!adx.equals("4"))
                return;

            String yoyiCookie = bidLog.getUser().getUserYyid();
            List<String> crowdTag = bidLog.getUser().getUserCrowdTagsList();

            boolean hasSpecificTag = false;

            for(String tag: crowdTag){
                if (tag.equals("7020009")){
                    value_result.set(yoyiCookie);
                    context.write(NullWritable.get(),value_result);
                    validOutputCt.increment(1);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"ExtractLingJiCookie");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(ExtractLingJiCookie.class);
//        job.setReducerClass(ExtractLingJiCookieReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String originalBidPath = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, originalBidPath, TextInputFormat.class, ExtractLingJiCookieMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
