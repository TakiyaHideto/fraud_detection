package com.mr.ad_hoc_function;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/3/8.
 */
public class ExtractDataForFrdDetCo {
    public static class MonitorDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("MonitorDataMapper", "validOutputCt");

            // initialization
            try {
                String line = value.toString();
                String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
                String yoyiCookie = elementInfo[1];
                String duration = elementInfo[elementInfo.length-1];
                String timestamp = elementInfo[0];

                key_result.set(yoyiCookie);
                value_result.set(timestamp +
                        Properties.Base.CTRL_A + duration +
                        Properties.Base.CTRL_A + "monitor");
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                context.getCounter("MonitorDataMapper","arrayIndexOutOfBoundsExceptionCt").increment(1);
            }
        }
    }

    public static class BasedataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("BasedataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String yoyiCookie = elementInfo[42];
            key_result.set(yoyiCookie);
            value_result.set(line + Properties.Base.CTRL_A +"basedata_fraud");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class ExtractDataForFrdDetCoReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputBasedataCt = context.getCounter("ExtractDataForFrdDetCoReducer", "validOutputBasedataCt");
            Counter validOutputMonitorCt = context.getCounter("ExtractDataForFrdDetCoReducer", "validOutputMonitorCt");

            LinkedList<String> basedataList = new LinkedList<String>();
            LinkedList<String> monitorList = new LinkedList<String>();
            Boolean hasMonitorInfo = false;
            Boolean hasBasedataInfo = false;

            for (Text value: values){
                String valueString = value.toString();
                if (valueString.contains("basedata_fraud")){
                    basedataList.add(valueString.substring(0,valueString.length()-"basedata_fraud".length()));
                    hasBasedataInfo = true;
                } else if (valueString.contains("monitor")){
                    String timestamp = valueString.split(Properties.Base.CTRL_A,-1)[0];
                    String monitorDuration = valueString.split(Properties.Base.CTRL_A,-1)[1];
                    monitorList.add(key.toString() +
                                Properties.Base.CTRL_A + timestamp +
                                Properties.Base.CTRL_A + monitorDuration);
                    hasMonitorInfo = true;
                }
            }

            if (hasBasedataInfo && hasMonitorInfo){
                for (String info: basedataList){
                    value_result.set("basedata"+Properties.Base.BS_SEPARATOR_TAB+info);
                    context.write(NullWritable.get(),value_result);
                    validOutputBasedataCt.increment(1);
                }
                for (String info: monitorList){
                    value_result.set("monitor"+Properties.Base.BS_SEPARATOR_TAB+info);
                    context.write(NullWritable.get(), value_result);
                    validOutputMonitorCt.increment(1);
                }
            } else if (hasBasedataInfo && !hasMonitorInfo){
                if (Math.random()<0.1){
                    for (String info: basedataList){
                        value_result.set("basedata"+Properties.Base.BS_SEPARATOR_TAB+info);
                        context.write(NullWritable.get(),value_result);
                        validOutputBasedataCt.increment(1);
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"ExtractDataForFrdDetCo");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(ExtractDataForFrdDetCo.class);
        job.setReducerClass(ExtractDataForFrdDetCoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedata = otherArgs[1];
        String monitorData = otherArgs[2];

        CommUtil.addInputFileComm(job, fs, monitorData, TextInputFormat.class, MonitorDataMapper.class);
        CommUtil.addInputFileComm(job, fs, basedata, TextInputFormat.class, BasedataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
