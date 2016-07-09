package com.mr.temp;

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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by TakiyaHideto on 16/4/27.
 */
public class TagCookieNum {
    public static class TagCookieNumMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException {
            // create counters
            Counter validInputCt = context.getCounter("TagCookieNumMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("TagCookieNumMapper", "validOutputCt");

            validInputCt.increment(1);

            // initialization
            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);
            String yoyiCookie = elements[0];
            String[] natureList = elements[1].split(Properties.Base.CTRL_B,-1);
            String[] tagList = elements[2].split(Properties.Base.CTRL_B,-1);

            for (String tag: tagList){
                key_result.set(tag);
                value_result.set(yoyiCookie);
                context.write(key_result,value_result);
                validOutputCt.increment(1);
            }
            for (String nature: natureList){
                key_result.set(nature);
                value_result.set(yoyiCookie);
                context.write(key_result,value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static class TagCookieNumReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("TagCookieNumReducer", "validOutputCt");

            int counter = 0;
            for (Text value: values){
                counter++;
            }

            value_result.set(key.toString() + Properties.Base.BS_SEPARATOR_TAB + String.valueOf(counter));
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

        Job job = new Job(conf,"TagCookieNum");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(TagCookieNum.class);
        job.setReducerClass(TagCookieNumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String retargetingData = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, retargetingData, TextInputFormat.class, TagCookieNumMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
