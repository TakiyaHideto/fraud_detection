package com.mr.effect_analysis;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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
 * Created by TakiyaHideto on 16/5/3.
 */
public class ExtractDomainValuationData {
    public static class ExtractDomainValuationDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ExtractDomainValuationDataMapper", "validInput");
            Counter validOutputCt = context.getCounter("ExtractDomainValuationDataMapper", "validOutputCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("ExtractDomainValuationDataMapper", "arrayIndexOutOfBoundsExceptionCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);

            try {
                String cookie = elementInfo[1];
                String sessionId = elementInfo[14];
                String referUrl = elementInfo[6];
                String isStart = elementInfo[15];
                String isEnd = elementInfo[16];
                String keepTime = elementInfo[17];
                String deep = elementInfo[19];
                String referType = elementInfo[7];
                String referDomain = UrlUtil.getUrlDomain(referUrl);

//                if (!referType.equals("0"))
//                    return;

                key_result.set(sessionId);
                value_result.set(referDomain +
                        Properties.Base.CTRL_A + isStart +
                        Properties.Base.CTRL_A + isEnd +
                        Properties.Base.CTRL_A + keepTime +
                        Properties.Base.CTRL_A + deep +
                        Properties.Base.CTRL_A + referType +
                        Properties.Base.CTRL_A + cookie);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                arrayIndexOutOfBoundsExceptionCt.increment(1);
            }
        }
    }

    public static class ExtractDomainValuationDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("ExtractDomainValuationDataReducer", "validOutputCt");
            Counter broswerDeepNullCt = context.getCounter("ExtractDomainValuationDataReducer", "broswerDeepNullCt");

            int keepTimeSum = 0;
            String secondBounce = "0";
            String referDomain = "";
            String browserDeep = "";
            String firstReferDomain = "";
            String cookie = "";
            double keepTimeDeepRatio = 0.0;

            for (Text value: values){
                String valueString = value.toString();
                String[] elementsInfo = valueString.split(Properties.Base.CTRL_A,-1);
                String domain = elementsInfo[0];
                String isStart = elementsInfo[1];
                String isEnd = elementsInfo[2];
                String keepTime = elementsInfo[3];
                String deep = elementsInfo[4];
                String referType = elementsInfo[5];

                cookie = elementsInfo[6];
                keepTimeSum += Integer.parseInt(keepTime);
                if (isStart.equals("1")){
                    referDomain = domain;
                    if (!keepTime.equals("0")){
                        secondBounce = "1";
                    }
                    if (referType.equals("0") ||
                            referType.equals("1") ||
                            referType.equals("4")){
                        firstReferDomain = domain;
                    } else {
                        return;
                    }
                }
                if (isEnd.equals("1")){
                    browserDeep = deep;
                }
            }

            if (!browserDeep.equals("")) {
                keepTimeDeepRatio = (double) keepTimeSum / Double.parseDouble(browserDeep);
            } else {
                broswerDeepNullCt.increment(1);
                return;
            }
            if (referDomain.equals("")){
                referDomain = "unknown_domain";
            }
            if (firstReferDomain.equals("")){
                firstReferDomain = "monitor_site";
            }
            if (cookie.equals("")){
                cookie = "unknown_cookie";
            }

            value_result.set(cookie +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(keepTimeSum) +
                    Properties.Base.BS_SEPARATOR_TAB + browserDeep +
                    Properties.Base.BS_SEPARATOR_TAB + secondBounce +
                    Properties.Base.BS_SEPARATOR_TAB + String.valueOf(keepTimeDeepRatio));
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

        Job job = new Job(conf,"ExtractDomainValuationData");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(ExtractDomainValuationData.class);
        job.setReducerClass(ExtractDomainValuationDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<61;i++) {
            CommUtil.addInputFileComm(job, fs, bidLogFile + "/" + currentDate,
                    TextInputFormat.class, ExtractDomainValuationDataMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
