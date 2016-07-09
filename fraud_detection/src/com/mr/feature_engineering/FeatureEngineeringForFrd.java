package com.mr.feature_engineering;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/5/30.
 */
public class FeatureEngineeringForFrd {
    public static class FeatureEngineeringForFrdMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> oriFeatureIndexMap = new HashMap<String,String>();
        private HashSet<String> feaKeptSet = new HashSet<String>();

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

            this.feaKeptSet.add("bid_way");
            this.feaKeptSet.add("campCateId");
            this.feaKeptSet.add("campSubCateId");
            this.feaKeptSet.add("width");
            this.feaKeptSet.add("height");
            this.feaKeptSet.add("domain");
            this.feaKeptSet.add("host");
            this.feaKeptSet.add("page_title");
            this.feaKeptSet.add("adzone_id");
            this.feaKeptSet.add("adzone_position");
            this.feaKeptSet.add("reserve_price");
            this.feaKeptSet.add("weekday");
            this.feaKeptSet.add("hour");
            this.feaKeptSet.add("browser");
            this.feaKeptSet.add("os");
            this.feaKeptSet.add("language");
            this.feaKeptSet.add("area_id");
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("FeatureEngineeringForFrdMapper", "validInput");
            Counter validOutputCt = context.getCounter("FeatureEngineeringForFrdMapper", "validOutputCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            int count = 0;

            for (String feature: elementInfo){
                if (this.feaKeptSet.contains(this.oriFeatureIndexMap.get(String.valueOf(count)))) {
                    if (feature.contains(Properties.Base.CTRL_B)) {
                        for (String subFea: feature.split(Properties.Base.CTRL_B)){
                            String feaVal = this.oriFeatureIndexMap.get(String.valueOf(count)) +
                                    Properties.Base.CTRL_A + subFea;
                            key_result.set(feaVal);
                            value_result.set("SUB_FEATURE");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    } else {
                        String feaVal = this.oriFeatureIndexMap.get(String.valueOf(count)) +
                                Properties.Base.CTRL_A + feature;
                        key_result.set(feaVal);
                        value_result.set("SINGLE_FEATURE");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
                count++;
            }
        }
    }

    public static class FeatureEngineeringForFrdReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private int FEATURE_VALUE_FREQ = 1;

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("FeatureEngineeringForFrdReducer", "validOutputCt");

            // 这里做特征工程

            int freq = 0;

            for (Text value: values){
                freq++;
            }

            if (freq>=this.FEATURE_VALUE_FREQ){
                value_result.set(key.toString() +
                        Properties.Base.CTRL_A + String.valueOf(freq));
                context.write(NullWritable.get(), value_result);
                validOutputCt.increment(1);
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

        Job job = new Job(conf,"FeatureEngineeringForFrd");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(FeatureEngineeringForFrd.class);
        job.setReducerClass(FeatureEngineeringForFrdReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String fraudData = otherArgs[1];
        String baiduData = otherArgs[2];
        String wasuData = otherArgs[3];
        String startDate = otherArgs[4];
        String endDate = otherArgs[5];

        while (!startDate.equals(endDate)) {
            CommUtil.addInputFileComm(job, fs, fraudData+"/log_date="+startDate, TextInputFormat.class, FeatureEngineeringForFrdMapper.class);
            CommUtil.addInputFileComm(job, fs, baiduData+"/log_date="+startDate, TextInputFormat.class, FeatureEngineeringForFrdMapper.class);
            CommUtil.addInputFileComm(job, fs, wasuData+"/log_date="+startDate, TextInputFormat.class, FeatureEngineeringForFrdMapper.class);
            startDate = DateUtil.getSpecifiedDayAfter(startDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
