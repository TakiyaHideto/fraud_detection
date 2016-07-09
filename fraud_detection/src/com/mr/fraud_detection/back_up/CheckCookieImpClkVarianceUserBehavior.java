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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/1/12.
 */
public class CheckCookieImpClkVarianceUserBehavior {
    public static class CheckCookieImpClkVarianceUserBehaviorMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> ipSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String domainSetFilePath1 = (String)context.getConfiguration().get("ipSetPath");
            readFilterFile(fs,domainSetFilePath1,this.ipSet);
        }

        public static void readFilterFile(FileSystem fs, String path, HashSet<String> filterSet)
                throws FileNotFoundException, IOException{
            filterSet.add("113.238.162.143");
            filterSet.add("113.236.21.53");
            filterSet.add("113.235.104.92");
            filterSet.add("114.44.1.84");
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckCookieImpClkVarianceUserBehaviorMapper", "validInput");
            Counter protoBufferExceptCt = context.getCounter("CheckCookieImpClkVarianceUserBehaviorMapper","protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("CheckCookieImpClkVarianceUserBehaviorMapper","otherExceptionCt");
            Counter nullCookieInfoCt = context.getCounter("CheckCookieImpClkVarianceUserBehaviorMapper", "nullCookieInfoCt");

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
            String ip = bidLog.getUser().getUserIp();
            if(!this.ipSet.contains(ip))
                return;
            String timestamp = String.valueOf(bidLog.getTimestamp());
            String url = bidLog.getPage().getPageUrl();
            key_result.set(ip);
            value_result.set(timestamp + Properties.Base.BS_SEPARATOR + url);
            context.write(key_result, value_result);
        }
    }

    public static class CheckCookieImpClkVarianceUserBehaviorReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckCookieImpClkVarianceUserBehaviorReducer", "validOutputCt");

            HashMap<String,String> timeUrlMap = new HashMap<String,String>();
            for (Text value: values){
                String[] ele = value.toString().split(Properties.Base.BS_SEPARATOR,-1);
                timeUrlMap.put(ele[0],ele[1]);
            }
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
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CheckCookieImpClkVarianceUserBehavior");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckCookieImpClkVarianceUserBehavior.class);
        job.setReducerClass(CheckCookieImpClkVarianceUserBehaviorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, bidLogFile, TextInputFormat.class, CheckCookieImpClkVarianceUserBehaviorMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
