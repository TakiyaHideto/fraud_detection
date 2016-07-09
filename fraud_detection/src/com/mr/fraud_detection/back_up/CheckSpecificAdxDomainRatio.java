package com.mr.fraud_detection.back_up;

import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.TextMessageCodec;
import com.mr.utils.UrlUtil;
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
 * Created by TakiyaHideto on 16/2/23.
 */
public class CheckSpecificAdxDomainRatio {
    public static class CheckSpecificAdxDomainRatioMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckSpecificAdxDomainRatioMapper", "validInput");
            Counter protoBufferExceptCt = context.getCounter("CheckSpecificAdxDomainRatioMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckSpecificAdxDomainRatioMapper","otherExceptionCt");

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

            String domain = UrlUtil.getUrlDomain(bidLog.getPage().getPageUrl());
            String adx = bidLog.getExchange().getAdxId();

            // output key-value
            // key_result.set(yoyi_cookie);
            key_result.set(domain);
            value_result.set(adx);
            context.write(key_result, value_result);
        }
    }

    public static class CheckSpecificAdxDomainRatioReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckSpecificAdxDomainRatioReducer", "validOutputCt");
            Counter specificAdxDomainCt = context.getCounter("CheckSpecificAdxDomainRatioReducer", "specificAdxDomainCt");
            Counter allAdxDomainCt = context.getCounter("CheckSpecificAdxDomainRatioReducer", "allAdxDomainCt");

            Boolean isSpecificDomain = false;

            for (Text value: values){
                String adx = value.toString();
                if (adx.equals("1") || adx.equals("2") || adx.equals("7"))
                    isSpecificDomain = true;
            }
            if (isSpecificDomain)
                specificAdxDomainCt.increment(1);
            else
                allAdxDomainCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckSpecificAdxDomainRatio");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckSpecificAdxDomainRatio.class);
        job.setReducerClass(CheckSpecificAdxDomainRatioReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, bidLogFile, TextInputFormat.class, CheckSpecificAdxDomainRatioMapper.class,"all");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
