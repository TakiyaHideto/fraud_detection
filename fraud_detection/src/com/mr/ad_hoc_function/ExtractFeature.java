package com.mr.ad_hoc_function;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/3/9.
 */
public class ExtractFeature {
    public static class ExtractFeatureMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("AddFilterBlackListForBasedataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("AddFilterBlackListForBasedataMapper", "validOutputCt");


            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.CTRL_A, -1);

            String domain = elementsInfo[22];
            key_result.set(domain+Properties.Base.CTRL_A+"domain");
            value_result.set("domain");
            context.write(key_result,value_result);

            String host = elementsInfo[23];
            key_result.set(host+Properties.Base.CTRL_A+"host");
            value_result.set("host");
            context.write(key_result,value_result);

            String url = elementsInfo[24];
            key_result.set(url+Properties.Base.CTRL_A+"url");
            value_result.set("url");
            context.write(key_result,value_result);

            String gender = elementsInfo[43];
            key_result.set(gender+Properties.Base.CTRL_A+"gender");
            value_result.set("gender");
            context.write(key_result,value_result);

            String profile = elementsInfo[44];
            for (String tag: profile.split(Properties.Base.CTRL_B)){
                key_result.set(tag+Properties.Base.CTRL_A+"tag");
                value_result.set("tag");
                context.write(key_result,value_result);
            }
            String ip = elementsInfo[40];
            key_result.set(ip+Properties.Base.CTRL_A+"ip");
            value_result.set("ip");
            context.write(key_result,value_result);

            ///////////////////////////////////////////////
            // following features belong to mobile basedata
            String devicePlatform = elementsInfo[46];
            key_result.set(devicePlatform+Properties.Base.CTRL_A+"devicePlatform");
            value_result.set("devicePlatform");
            context.write(key_result,value_result);

            String deviceOs = elementsInfo[47];
            key_result.set(deviceOs+Properties.Base.CTRL_A+"deviceOs");
            value_result.set("deviceOs");
            context.write(key_result,value_result);

            String deviceOsVersion = elementsInfo[48];
            key_result.set(deviceOsVersion+Properties.Base.CTRL_A+"deviceOsVersion");
            value_result.set("deviceOsVersion");
            context.write(key_result,value_result);

            String deviceBrand = elementsInfo[49];
            key_result.set(deviceBrand+Properties.Base.CTRL_A+"deviceBrand");
            value_result.set("deviceBrand");
            context.write(key_result,value_result);

            String deviceModel = elementsInfo[50];
            key_result.set(deviceModel+Properties.Base.CTRL_A+"deviceModel");
            value_result.set("deviceModel");
            context.write(key_result,value_result);

            String deviceLocation = elementsInfo[51];
            key_result.set(deviceLocation+Properties.Base.CTRL_A+"deviceLocation");
            value_result.set("deviceLocation");
            context.write(key_result,value_result);

            String deviceResultion = elementsInfo[52];
            key_result.set(deviceResultion+Properties.Base.CTRL_A+"deviceResultion");
            value_result.set("deviceResultion");
            context.write(key_result,value_result);

            String networkType = elementsInfo[53];
            key_result.set(networkType+Properties.Base.CTRL_A+"networkType");
            value_result.set("networkType");
            context.write(key_result,value_result);

            String networkCarrier = elementsInfo[54];
            key_result.set(networkCarrier+Properties.Base.CTRL_A+"networkCarrier");
            value_result.set("networkCarrier");
            context.write(key_result,value_result);

            String appName = elementsInfo[57];
            key_result.set(appName+Properties.Base.CTRL_A+"appName");
            value_result.set("appName");
            context.write(key_result,value_result);

            String appCategories = elementsInfo[58];
            key_result.set(appCategories+Properties.Base.CTRL_A+"appCategories");
            value_result.set("appCategories");
            context.write(key_result,value_result);

            validInputCt.increment(1);

        }
    }

    public static class ExtractFeatureReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private MultipleOutputs<NullWritable, Text> mos;

        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable,Text>(context);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            String keyString = key.toString();
            if (keyString.contains("domain")) {
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("domain",NullWritable.get(),value_result,"domain");
            } else if (keyString.contains("host")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("host",NullWritable.get(),value_result,"host");
            } else if (keyString.contains("url")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("url",NullWritable.get(),value_result,"url");
            } else if (keyString.contains("gender")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("gender",NullWritable.get(),value_result,"gender");
            } else if (keyString.contains("tag")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("tag",NullWritable.get(),value_result,"tag");
            } else if (keyString.contains("ip")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("ip",NullWritable.get(),value_result,"ip");
            }
            ///////////////////////////////////////////////
            // following features belong to mobile basedata
            else if (keyString.contains("devicePlatform")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("devicePlatform",NullWritable.get(),value_result,"devicePlatform");
            }
            else if (keyString.contains("deviceOs")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceOs",NullWritable.get(),value_result,"deviceOs");
            }
            else if (keyString.contains("deviceOsVersion")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceOsVersion",NullWritable.get(),value_result,"deviceOsVersion");
            }
            else if (keyString.contains("deviceBrand")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceBrand",NullWritable.get(),value_result,"deviceBrand");
            }
            else if (keyString.contains("deviceModel")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceModel",NullWritable.get(),value_result,"deviceModel");
            }
            else if (keyString.contains("deviceLocation")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceLocation",NullWritable.get(),value_result,"deviceLocation");
            }
            else if (keyString.contains("deviceResultion")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("deviceResultion",NullWritable.get(),value_result,"deviceResultion");
            }
            else if (keyString.contains("networkType")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("networkType",NullWritable.get(),value_result,"networkType");
            }
            else if (keyString.contains("networkCarrier")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("networkCarrier",NullWritable.get(),value_result,"networkCarrier");
            }
            else if (keyString.contains("appName")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("appName",NullWritable.get(),value_result,"appName");
            }
            else if (keyString.contains("appCategories")){
                value_result.set(keyString.split(Properties.Base.CTRL_A,-1)[0]);
                mos.write("appCategories",NullWritable.get(),value_result,"appCategories");
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

        Job job = new Job(conf,"ExtractFeature");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(ExtractFeature.class);
        job.setReducerClass(ExtractFeatureReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        MultipleOutputs.addNamedOutput(job,"domain",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"host",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"url",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"gender",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"tag",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"ip",TextOutputFormat.class,NullWritable.class, Text.class);
        ///////////////////////////////////////////////
        // following features belong to mobile basedata
        MultipleOutputs.addNamedOutput(job,"devicePlatform",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceOs",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceOsVersion",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceBrand",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceModel",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceLocation",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"deviceResultion",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"networkType",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"networkCarrier",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"appName",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"appCategories",TextOutputFormat.class,NullWritable.class, Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPcPath = otherArgs[1];
        String currentData = otherArgs[2];

        for (int i=0;i<15;i++) {
            CommUtil.addInputFileComm(job, fs, basedataPcPath + "/log_date=" + currentData, TextInputFormat.class, ExtractFeatureMapper.class);
            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
