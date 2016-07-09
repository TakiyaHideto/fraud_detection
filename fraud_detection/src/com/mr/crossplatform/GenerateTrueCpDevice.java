package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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
 * Created by TakiyaHideto on 16/3/8.
 */
public class GenerateTrueCpDevice {
    public static class GenerateTrueCpDeviceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("GenerateImitatedCpDeviceMapper", "validOutputCt");
            Counter illegalRandomCt = context.getCounter("GenerateImitatedCpDeviceMapper","illegalRandomCt");

            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.BS_SEPARATOR_TAB,-1);
            String device = elementInfo[0].split(Properties.Base.CTRL_A,-1)[0];
            String yoyiCookie = elementInfo[1].split(Properties.Base.CTRL_B,-1)[0];

            key_result.set(device+Properties.Base.CTRL_A+yoyiCookie);
            value_result.set("1");
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class GenerateTrueCpDeviceReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("GenerateImitatedCpDeviceReducer", "validOutputCt");

            value_result.set(key.toString());
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

        Job job = new Job(conf,"GenerateTrueCpDevice");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(GenerateTrueCpDevice.class);
        job.setReducerClass(GenerateTrueCpDeviceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedata = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<15;i++) {
            CommUtil.addInputFileComm(job, fs, basedata + "/" + currentDate + "/BaiduCP",
                    TextInputFormat.class, GenerateTrueCpDeviceMapper.class);
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
