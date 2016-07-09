package com.mr.feature_engineering;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.FeaEngUtil;
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
import org.apache.hadoop.util.hash.Hash;

import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by TakiyaHideto on 16/2/4.
 */
public class OutputNewFormRecords {
    public static class OutputNewFormRecordsFraudMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> oriFeatureIndexMap = new HashMap<String,String>();
        private HashSet<String> feaKeptSet = new HashSet<String>();
        private HashMap<String,String> featureMapping = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            this.oriFeatureIndexMap.put("0","session_id");
            this.oriFeatureIndexMap.put("1","yoyi_cost");
            this.oriFeatureIndexMap.put("2","clk");
            this.oriFeatureIndexMap.put("3","reach");
            this.oriFeatureIndexMap.put("4","action");
            this.oriFeatureIndexMap.put("5","action_monitor_id");
            this.oriFeatureIndexMap.put("6","bid_way");
            this.oriFeatureIndexMap.put("7","algo_data");
            this.oriFeatureIndexMap.put("8","account_id");
            this.oriFeatureIndexMap.put("9","campaign_id");
            this.oriFeatureIndexMap.put("10","camp_cate_id");
            this.oriFeatureIndexMap.put("11","camp_sub_cate_id");
            this.oriFeatureIndexMap.put("12","order_id");
            this.oriFeatureIndexMap.put("13","ad_id");
            this.oriFeatureIndexMap.put("14","width");
            this.oriFeatureIndexMap.put("15","height");
            this.oriFeatureIndexMap.put("16","filesize");
            this.oriFeatureIndexMap.put("17","extname");
            this.oriFeatureIndexMap.put("18","adx_id");
            this.oriFeatureIndexMap.put("19","site_cate_id");
            this.oriFeatureIndexMap.put("20","content_cate_id");
            this.oriFeatureIndexMap.put("21","yoyi_cate_id");
            this.oriFeatureIndexMap.put("22","domain");
            this.oriFeatureIndexMap.put("23","host");
            this.oriFeatureIndexMap.put("24","url");
            this.oriFeatureIndexMap.put("25","refer_url");
            this.oriFeatureIndexMap.put("26","page_title");
            this.oriFeatureIndexMap.put("27","adzone_id");
            this.oriFeatureIndexMap.put("28","adzone_position");
            this.oriFeatureIndexMap.put("29","reserve_price");
            this.oriFeatureIndexMap.put("30","bid_timestamp");
            this.oriFeatureIndexMap.put("31","bid_date");
            this.oriFeatureIndexMap.put("32","weekday");
            this.oriFeatureIndexMap.put("33","hour");
            this.oriFeatureIndexMap.put("34","minute");
            this.oriFeatureIndexMap.put("35","user_agent");
            this.oriFeatureIndexMap.put("36","browser");
            this.oriFeatureIndexMap.put("37","os");
            this.oriFeatureIndexMap.put("38","language");
            this.oriFeatureIndexMap.put("39","area_id");
            this.oriFeatureIndexMap.put("40","ip");
            this.oriFeatureIndexMap.put("41","adx_cookie");
            this.oriFeatureIndexMap.put("42","yoyi_cookie");
            this.oriFeatureIndexMap.put("43","gender");
            this.oriFeatureIndexMap.put("44","profile");

            Configuration conf = context.getConfiguration();
            String featureMap = context.getConfiguration().get("featureMap");
            fs = FileSystem.get(conf);
            loadCache(fs, featureMap, this.featureMapping);
        }

        public static void loadCache(FileSystem fs,String path, HashMap<String,String> featureMapping)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] elements = line.split(Properties.Base.CTRL_A);
                featureMapping.put(elements[1]+Properties.Base.CTRL_A,elements[0]);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CreateFeatureMapMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("CreateFeatureMapMapper", "validOutputCt");

            String line = value.toString();

            String label = "1";

            int count = 0;
            String sample = "";

            for (String feature: line.split(Properties.Base.CTRL_A)){
                if (feature.contains(Properties.Base.CTRL_B)){
                    for (String subFea: feature.split(Properties.Base.CTRL_B)){
                        String featureName = this.oriFeatureIndexMap.get(String.valueOf(String.valueOf(count)));
                        if (this.featureMapping.containsKey(featureName+Properties.Base.CTRL_A+subFea)){
                            String index = this.featureMapping.get(featureName + Properties.Base.CTRL_A + subFea);
                            sample += index+":1";
                        }
                    }
                } else {
                    String featureName = this.oriFeatureIndexMap.get(String.valueOf(String.valueOf(count)));
                    if (this.featureMapping.containsKey(featureName+Properties.Base.CTRL_A+feature)){
                        String index = this.featureMapping.get(featureName + Properties.Base.CTRL_A + feature);
                        sample += Properties.Base.BS_SEPARATOR_SPACE + index+":1";
                    }
                }
                count++;
            }

            if (!sample.equals("")){
                sample = label + sample;
                key_result.set(String.valueOf(Math.random()));
                value_result.set(sample);
                context.write(key_result, value_result);
            }
        }
    }

    public static class OutputNewFormRecordsNormalMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> oriFeatureIndexMap = new HashMap<String,String>();
        private HashSet<String> feaKeptSet = new HashSet<String>();
        private HashMap<String,String> featureMapping = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            this.oriFeatureIndexMap.put("0","session_id");
            this.oriFeatureIndexMap.put("1","yoyi_cost");
            this.oriFeatureIndexMap.put("2","clk");
            this.oriFeatureIndexMap.put("3","reach");
            this.oriFeatureIndexMap.put("4","action");
            this.oriFeatureIndexMap.put("5","action_monitor_id");
            this.oriFeatureIndexMap.put("6","bid_way");
            this.oriFeatureIndexMap.put("7","algo_data");
            this.oriFeatureIndexMap.put("8","account_id");
            this.oriFeatureIndexMap.put("9","campaign_id");
            this.oriFeatureIndexMap.put("10","camp_cate_id");
            this.oriFeatureIndexMap.put("11","camp_sub_cate_id");
            this.oriFeatureIndexMap.put("12","order_id");
            this.oriFeatureIndexMap.put("13","ad_id");
            this.oriFeatureIndexMap.put("14","width");
            this.oriFeatureIndexMap.put("15","height");
            this.oriFeatureIndexMap.put("16","filesize");
            this.oriFeatureIndexMap.put("17","extname");
            this.oriFeatureIndexMap.put("18","adx_id");
            this.oriFeatureIndexMap.put("19","site_cate_id");
            this.oriFeatureIndexMap.put("20","content_cate_id");
            this.oriFeatureIndexMap.put("21","yoyi_cate_id");
            this.oriFeatureIndexMap.put("22","domain");
            this.oriFeatureIndexMap.put("23","host");
            this.oriFeatureIndexMap.put("24","url");
            this.oriFeatureIndexMap.put("25","refer_url");
            this.oriFeatureIndexMap.put("26","page_title");
            this.oriFeatureIndexMap.put("27","adzone_id");
            this.oriFeatureIndexMap.put("28","adzone_position");
            this.oriFeatureIndexMap.put("29","reserve_price");
            this.oriFeatureIndexMap.put("30","bid_timestamp");
            this.oriFeatureIndexMap.put("31","bid_date");
            this.oriFeatureIndexMap.put("32","weekday");
            this.oriFeatureIndexMap.put("33","hour");
            this.oriFeatureIndexMap.put("34","minute");
            this.oriFeatureIndexMap.put("35","user_agent");
            this.oriFeatureIndexMap.put("36","browser");
            this.oriFeatureIndexMap.put("37","os");
            this.oriFeatureIndexMap.put("38","language");
            this.oriFeatureIndexMap.put("39","area_id");
            this.oriFeatureIndexMap.put("40","ip");
            this.oriFeatureIndexMap.put("41","adx_cookie");
            this.oriFeatureIndexMap.put("42","yoyi_cookie");
            this.oriFeatureIndexMap.put("43","gender");
            this.oriFeatureIndexMap.put("44","profile");

            Configuration conf = context.getConfiguration();
            String featureMap = context.getConfiguration().get("featureMap");
            fs = FileSystem.get(conf);
            loadCache(fs, featureMap, this.featureMapping);
        }

        public static void loadCache(FileSystem fs,String path, HashMap<String,String> featureMapping)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] elements = line.split(Properties.Base.CTRL_A);
                featureMapping.put(elements[1]+Properties.Base.CTRL_A+elements[2],elements[0]);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CreateFeatureMapMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("CreateFeatureMapMapper", "validOutputCt");
            String line = value.toString();

            String label = "0";

            int count = 0;
            String sample = "";

            for (String feature: line.split(Properties.Base.CTRL_A)){
                if (feature.contains(Properties.Base.CTRL_B)){
                    for (String subFea: feature.split(Properties.Base.CTRL_B)){
                        String featureName = this.oriFeatureIndexMap.get(String.valueOf(String.valueOf(count)));
                        if (this.featureMapping.containsKey(featureName+Properties.Base.CTRL_A+subFea)){
                            String index = this.featureMapping.get(featureName + Properties.Base.CTRL_A + subFea);
                            sample += index+":1";
                        }
                    }
                } else {
                    String featureName = this.oriFeatureIndexMap.get(String.valueOf(String.valueOf(count)));
                    if (this.featureMapping.containsKey(featureName+Properties.Base.CTRL_A+feature)){
                        String index = this.featureMapping.get(featureName + Properties.Base.CTRL_A + feature);
                        sample += Properties.Base.BS_SEPARATOR_SPACE + index+":1";
                    }
                }
                count++;
            }

            if (!sample.equals("")){
                sample = label + sample;
                key_result.set(String.valueOf(Math.random()));
                value_result.set(sample);
                context.write(key_result, value_result);
            }
        }
    }

    public static class OutputNewFormRecordsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            for (Text value: values){
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 6){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"OutputNewFormRecords");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        job.getConfiguration().set("mapreduce.map.memory.mb ", "4000");
        job.getConfiguration().set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx4000000000");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(OutputNewFormRecords.class);
        job.setReducerClass(OutputNewFormRecordsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String fraudBaseData = otherArgs[1];
        String normalBaseData = otherArgs[2];
        String normalBaiduCpBaseData = otherArgs[3];
        String featureMap = otherArgs[4];
        String currentData = otherArgs[5];

        job.getConfiguration().set("featureMap",featureMap);
        currentData = DateUtil.getSpecifiedDayBefore(currentData);

        for (int i=0;i<7;i++){
            CommUtil.addInputFileComm(job, fs, fraudBaseData+"/log_date="+currentData, TextInputFormat.class, OutputNewFormRecordsFraudMapper.class);
            CommUtil.addInputFileComm(job, fs, normalBaseData+"/log_date="+currentData, TextInputFormat.class, OutputNewFormRecordsNormalMapper.class);
            CommUtil.addInputFileComm(job, fs, normalBaiduCpBaseData+"/log_date="+currentData, TextInputFormat.class, OutputNewFormRecordsNormalMapper.class);
            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}