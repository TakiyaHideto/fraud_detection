package com.mr.fraud_detection.back_up;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by TakiyaHideto on 16/1/27.
 */
public class CheckCid319UserBehaviour {

    public static class CheckCid319UserBehaviourMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckCid319UserBehaviourMapper", "validInput");
            Counter protoBufferExceptCt = context.getCounter("CheckCid319UserBehaviourMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckCid319UserBehaviourMapper","otherExceptionCt");
            Counter nullCookieInfoCt = context.getCounter("CheckCid319UserBehaviourMapper", "nullCookieInfoCt");

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
            String cookie = bidLog.getUser().getUserYyid();
            String timestamp = String.valueOf(bidLog.getTimestamp());
            String url = bidLog.getPage().getPageUrl();

            key_result.set(cookie);
            value_result.set(timestamp + Properties.Base.BS_SEPARATOR + url);
            context.write(key_result, value_result);
        }
    }

    public static class Cid319UserMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("Cid319UserMapper", "validOutputCt");

            // initialization
            String line = value.toString().trim();
            String cookie = line.split(Properties.Base.BS_SEPARATOR_TAB)[0];
            String tag = "Cid319";

            key_result.set(cookie);
            value_result.set(tag);
            context.write(key_result,value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CheckCid319UserBehaviourReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckCid319UserBehaviourReducer", "validOutputCt");

            HashMap<String,String> timeUrlMap = new HashMap<String,String>();
            Boolean flag = false;

            for (Text value: values){
                if (value.toString().contains("Cid319")){
                    flag = true;
                } else {
                    String[] ele = value.toString().split(Properties.Base.BS_SEPARATOR, -1);
                    timeUrlMap.put(ele[0], ele[1]);
                }
            }

            if (!flag)
                return;

            String[] timeList = timeUrlMap.keySet().toArray(new String[]{});
            Arrays.sort(timeList);
            String behaviourInfo = "";
            for (String time: timeList){
                String hour = "";
                String minute = "";
                try{
                    hour = String.valueOf(DateUtil.getTimeOfHour(Long.parseLong(time)));
                    minute = String.valueOf(DateUtil.getTimeMinute(Long.parseLong(time)));
                } catch (Exception e){
                    return;
                }
                behaviourInfo += hour+":"+minute + Properties.Base.BS_SEPARATOR_TAB + timeUrlMap.get(time) + "\n";
            }
            value_result.set(key.toString() + "\n" + behaviourInfo);
            context.write(NullWritable.get(), value_result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckCid319UserBehaviour");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckCid319UserBehaviour.class);
        job.setReducerClass(CheckCid319UserBehaviourReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];
        String cid319File = otherArgs[2];

        CommUtil.addInputFileComm(job, fs, bidLogFile, TextInputFormat.class, CheckCid319UserBehaviourMapper.class,"20");
        CommUtil.addInputFileComm(job, fs, cid319File, TextInputFormat.class, CheckCid319UserBehaviourMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
