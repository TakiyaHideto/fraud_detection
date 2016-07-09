package com.mr.temp;

import com.mr.config.Properties;
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
import java.util.Date;

/**
 * Created by TakiyaHideto on 16/5/9.
 */
public class temp {
    public static class BaiduCpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ClkMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ClkMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("ClkMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("ClkMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            String line = value.toString();
            String[] elements = line.split(Properties.Base.BS_SEPARATOR_TAB,-1);

            String mobile = elements[0].split(Properties.Base.CTRL_A,-1)[0];
            String cookie = elements[1].split(Properties.Base.CTRL_B,-1)[0];

            key_result.set(cookie + Properties.Base.CTRL_A + mobile);
            value_result.set("baidu_cp");
            context.write(key_result,value_result);
        }
    }

    public static class CpMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ClkMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ClkMapper", "validOutputCt");
            Counter protoBufferExceptCt = context.getCounter("ClkMapper", "protoBufferExceptCt");
            Counter otherExceptionCt = context.getCounter("ClkMapper", "otherExceptionCt");

            // initialization
            TextMessageCodec TMC = new TextMessageCodec();
            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

            String cookie = elements[0];
            String mobile = elements[1];
            key_result.set(cookie + Properties.Base.CTRL_A + mobile);
            value_result.set("yoyi_cp");
            context.write(key_result,value_result);
        }
    }

    public static class CheckOidFrdUserReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("Reducer", "validOutputCt");
            Counter isBaiduCt = context.getCounter("Reducer", "isBaiduCt");
            Counter bothCpCt = context.getCounter("Reducer","bothCpCt");

            boolean isYoyiCp = false;
            boolean isBaiduCp = false;

            for (Text value: values){
                if (value.toString().equals("yoyi_cp")){
                    isYoyiCp = true;
                }
                if (value.toString().equals("baidu_cp")){
                    isBaiduCp = true;
                }
                if (isBaiduCp && isYoyiCp){
                    break;
                }
            }

            if (isBaiduCp){
                isBaiduCt.increment(1);
            }
            if (isBaiduCp && isYoyiCp){
                bothCpCt.increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"temp");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(temp.class);
        job.setReducerClass(CheckOidFrdUserReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String yoyiCpFile = otherArgs[1];
        String baiduCpFile = otherArgs[2];
        String currentDate = otherArgs[3];

        currentDate = DateUtil.getSpecifiedDayBefore(currentDate);

        for (int i=0;i<8;i++){
            CommUtil.addInputFileComm(job,fs,baiduCpFile+"/"+currentDate+"/BaiduCP", TextInputFormat.class,BaiduCpMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }
        CommUtil.addInputFileComm(job, fs, yoyiCpFile, TextInputFormat.class, CpMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
