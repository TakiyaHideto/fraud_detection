package com.mr.code_backup;

/**
 * Created by TakiyaHideto on 15/12/3.
 */

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;

public class FetchDiyuanxinAddTag {
    public static class FetchDiyuanxinAddTagMapper extends Mapper<Object, Text, Text, Text>{
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("FetchDiyuanxinAddTagMapper", "validOutputCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            String line = value.toString().trim();
            String[] elementsInfo = line.split(Properties.Base.BS_SEPARATOR);
            if (elementsInfo.length != 5)
                return;
            String yoyiCookie = elementsInfo[0];

            key_result.set(yoyiCookie);
            value_result.set("DIYUANXIN_BENZ_1201");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class FetchDiyuanxinAddTagReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("FetchDiyuanxinAddTagReducer","validOutputCt");

            value_result.set(key.toString() + Properties.Base.BS_SEPARATOR + "DIYUANXIN_BENZ_1201");
            context.write(NullWritable.get(),value_result);
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

        Job job = new Job(conf,"FetchDiyuanxinAddTag");
        conf.set(Properties.Base.BS_DEFT_NAME, Properties.Base.BS_HDFS_NAME);
        conf.set(Properties.Base.BS_JOB_TRACKER, Properties.Base.BS_HDFS_TRACKER);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(FetchDiyuanxinAddTag.class);
        job.setReducerClass(FetchDiyuanxinAddTagReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String diyuanxinDataTagPath = otherArgs[1];
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        CommUtil.addInputFileCommSpecialJob(job,fs,diyuanxinDataTagPath,TextInputFormat.class,FetchDiyuanxinAddTagMapper.class,"addFilter");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
