package com.mr.ad_hoc_function;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by hideto on 16/3/9.
 */
public class CollectFrdDecDataForJiaodaEncodingSpecificFeat {
    public static class pcDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("pcDataMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("pcDataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.CTRL_A, -1);

            validInputCt.increment(1);

            if (Math.random()>0.2)
                return;
            // ip
            String ip = elementsInfo[40];

            key_result.set(ip);
            value_result.set(line);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class specificFeatMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters

            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.BS_SEPARATOR_TAB, -1);

            // ip
            String ip = elementsInfo[0];
            String index = elementsInfo[1];

            key_result.set(ip);
            value_result.set(index+Properties.Base.CTRL_A+"IpIndex");
            context.write(key_result, value_result);
        }
    }

    public static class CollectFrdDecDataForJiaodaEncodingSpecificFeatReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter illegalColumnCt = context.getCounter("CollectFrdDecDataForJiaoda", "illegalColumnCt");
            Counter validOutputCt = context.getCounter("CollectFrdDecDataForJiaoda", "validOutputCt");
            Counter nullIndexCt = context.getCounter("CollectFrdDecDataForJiaoda","nullIndexCt");

            LinkedList<String> list = new LinkedList<String>();

            String index = "";
            for (Text value: values){
                if (value.toString().contains("IpIndex")){
                    index = value.toString().split(Properties.Base.CTRL_A,-1)[0];
                } else {
                    list.add(value.toString());
                }
            }
            if (index.equals("")){
                nullIndexCt.increment(1);
                return;
            }

            for (String info: list){
                if (!info.contains("IpIndex")){
                    String[] elements = info.split(Properties.Base.CTRL_A,-1);
                    elements[40] = index;
                    String infoString = StringUtil.listToString(elements,Properties.Base.CTRL_A);
                    if (infoString.split(Properties.Base.CTRL_A,-1).length==45){
                        value_result.set(infoString);
                        context.write(NullWritable.get(), value_result);
                        validOutputCt.increment(1);
                    } else{
                        illegalColumnCt.increment(1);
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

        Job job = new Job(conf,"CollectFrdDecDataForJiaodaEncodingSpecificFeat");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectFrdDecDataForJiaodaEncodingSpecificFeat.class);
        job.setReducerClass(CollectFrdDecDataForJiaodaEncodingSpecificFeatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPcPath = otherArgs[1];
        String ipPath = otherArgs[2];

        CommUtil.addInputFileComm(job, fs, basedataPcPath, TextInputFormat.class, pcDataMapper.class);
        CommUtil.addInputFileComm(job, fs, ipPath, TextInputFormat.class, specificFeatMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
