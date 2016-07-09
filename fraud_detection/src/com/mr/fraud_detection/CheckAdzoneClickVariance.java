package com.mr.fraud_detection;

import com.mr.config.Properties;
import com.mr.utils.*;
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
import java.util.*;

/**
 * Created by hideto on 16/2/21.
 */
public class CheckAdzoneClickVariance {
    public static class CheckAdzoneClickVarianceMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CheckAdzoneClickVarianceMapper", "validInput");
            Counter validOutputCt = context.getCounter("CheckAdzoneClickVarianceMapper", "validOutputCt");

            validInputCt.increment(1);
            // initialization
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);

            String yoyiCost = elementInfo[1];
            String bidWay = elementInfo[6];
            if (!bidWay.equals("1")){
                return;
            }

            String adzoneId = elementInfo[27];
            String domain = elementInfo[22];
            String host = elementInfo[23];
            String clk = elementInfo[2];
            String campaignId = elementInfo[9];
            String weekday = elementInfo[32];
            String hour = elementInfo[33];
            String yoyiCookie = elementInfo[42];
            String ip = elementInfo[40];
            String log_date = elementInfo[31];

            // output key-value
            key_result.set(adzoneId);
            value_result.set(campaignId +
                    Properties.Base.CTRL_A + clk +
                    Properties.Base.CTRL_A + domain +
                    Properties.Base.CTRL_A + host +
                    Properties.Base.CTRL_A + weekday +
                    Properties.Base.CTRL_A + hour +
                    Properties.Base.CTRL_A + log_date +
                    Properties.Base.CTRL_A + ip +
                    Properties.Base.CTRL_A + yoyiCookie);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class CheckAdzoneClickVarianceReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckAdzoneClickVarianceReducer", "validOutputCt");

            HashMap<String,Integer> campaignImpMap = new HashMap<String, Integer>();
            HashMap<String,Integer> campaignClkMap = new HashMap<String, Integer>();
            HashMap<String,Double> campaignCtrMap = new HashMap<String, Double>();
            HashMap<String,Integer> periodClickMap = new HashMap<String,Integer>();
            HashMap<String,Integer> dateClkMap = new HashMap<String,Integer>();
            HashMap<String,Integer> dateImpMap = new HashMap<String,Integer>();
            HashMap<String,Double> dateCtrMap = new HashMap<String,Double>();
            LinkedHashMap<String,Integer> ipClkMap = new LinkedHashMap<String,Integer>();
            LinkedHashMap<String,Integer> ipClkMapTemp = new LinkedHashMap<String,Integer>();

            String domain = "";
            String host = "";
            String weekday = "";
            String hour = "";
            String log_date = "";
            String ip = "";
            String yoyiCookie = "";
            int impressionSum = 0;
            int clickSum = 0;

            for (Text value: values){
//                campaignId 0
//                clk 1
//                domain 2
//                host 3
//                weekday 4
//                hour 5
//                log_date 6
//                ip 7
//                yoyiCookie 8
                String[] valueInfo = value.toString().split(Properties.Base.CTRL_A,-1);
                String campaign = valueInfo[0];
                String clk = valueInfo[1];
                domain = valueInfo[2];
                host = valueInfo[3];
                weekday = valueInfo[4];
                hour = valueInfo[5];
                log_date = valueInfo[6];
                ip = valueInfo[7];
                yoyiCookie = valueInfo[8];

                if (clk.equals("1")){
                    String activityPeriod = DateUtil.getActivityPeriod(weekday,hour);
                    this.addInfoToMap(campaignClkMap,campaign);
                    this.addInfoToMap(periodClickMap,activityPeriod);
                    this.addInfoToMap(dateClkMap,log_date);
                    this.addInfoToMap(ipClkMap,ip);
                    clickSum ++;
                }
                this.addInfoToMap(campaignImpMap,campaign);
                this.addInfoToMap(dateImpMap,log_date);
                impressionSum ++;
            }

            // calculate global ctr
            double macroCtr = (double)clickSum/(double)impressionSum;

            // calculate #clk/#ip
            double clkPerIp = (double)clickSum/(double)ipClkMap.size();

            // calculate #clk/#date
            double clkPerDate = (double)clickSum/(double)dateClkMap.size();

            // calculate #clkDate sd
            double meanForClkData = MathUtil.calMeanMapInteger(dateClkMap);
            double sdForClkDate = MathUtil.calStandardDeviationMapInteger(dateClkMap, meanForClkData);

            // calculate ctr of each date
            dateCtrMap = MathUtil.calCtrForKey(dateImpMap, dateClkMap, dateCtrMap);
            // calculate mean of date ctr
            double meanDateCtr = MathUtil.calMeanMapDouble(dateCtrMap);
            // calculate sd of date ctr
            double sdDateCtr = MathUtil.calStandardDeviationMapDouble(dateCtrMap, meanDateCtr);

            // calculate ctr for every campaign
            campaignCtrMap = MathUtil.calCtrForKey(campaignImpMap, campaignClkMap, campaignCtrMap);
            // calculate mean of ctr
            double meanCampaignCtr = MathUtil.calMeanMapDouble(campaignCtrMap);
            // calculate sd of ctr
            double sdCampaignCtr = MathUtil.calStandardDeviationMapDouble(campaignCtrMap, meanCampaignCtr);

//            // calculate #clk/#date bias with poisson formula
//            double poissonClkDate = MathUtil.calculatePoissonDistribution((int) ((double) clickSum / (double) dateClkMap.size()), 117);
//
//            // calculate #clk/#ip bias with poisson formula
//            double poissonClkIp = MathUtil.calculatePoissonDistribution((int) ((double) clickSum / (double) ipClkMap.size()), 2);

            // calculate #IP with over 50% #clk ratio of total IP
            ipClkMapTemp = MathUtil.sortMap(ipClkMap);
            int ipClkedTotal = ipClkMap.size();
            int ipClkSum = MathUtil.calSumInteger(ipClkMapTemp);
            double ipRatioCumulative = (double)MathUtil.calRatioCumulative(ipClkMapTemp, 0.5, ipClkSum)/(double)ipClkedTotal;

            // find the key with max click number in activity period
            String keyMaxClickNumber = CommUtil.findKeyWithMaxValueInMap(periodClickMap);
            int maxClickNo = 0;
            if (!keyMaxClickNumber.equals(""))
                maxClickNo = periodClickMap.get(keyMaxClickNumber);

            if (clickSum != 0) {
//                value_result.set(key.toString() +
//                        Properties.Base.CTRL_A + domain +
//                        Properties.Base.CTRL_A + host +
//                        Properties.Base.CTRL_A + String.valueOf(standardVariance) +
//                        Properties.Base.CTRL_A + String.valueOf(totalCampaignNum) +
//                        Properties.Base.CTRL_A + String.valueOf(impressionSum) +
//                        Properties.Base.CTRL_A + String.valueOf(clickSum) +
//                        Properties.Base.CTRL_A + String.valueOf(macroCtr) +
//                        Properties.Base.CTRL_A + keyMaxClickNumber +
//                        Properties.Base.CTRL_A + String.valueOf(maxClickNo) +
//                        Properties.Base.CTRL_A + String.valueOf(dateSet.size()) +
//                        Properties.Base.CTRL_A + String.valueOf(ipClked.size()) +
//                        Properties.Base.CTRL_A + String.valueOf((double)clickSum/(double)ipClked.size()) +
//                        Properties.Base.CTRL_A + String.valueOf((double)clickSum/(double)dateSet.size()));
                domain = (domain.equals(""))?"unknown_domain":domain;
                value_result.set(key.toString()+
                        Properties.Base.BS_SEPARATOR_TAB + domain +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(impressionSum) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(clickSum) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sdCampaignCtr) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf((double) clickSum / (double) dateClkMap.size()) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sdForClkDate) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sdDateCtr) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf((double) clickSum / (double) ipClkMap.size()) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(ipRatioCumulative));
                context.write(NullWritable.get(), value_result);
                validOutputCt.increment(1);
            }
        }

        public static void addInfoToMap(HashMap<String,Integer> map, String keyInfo){
            if (map.containsKey(keyInfo)){
                int count = map.get(keyInfo);
                map.remove(keyInfo);
                map.put(keyInfo,count+1);
            } else {
                map.put(keyInfo,1);
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

        Job job = new Job(conf,"CheckAdzoneClickVariance");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(CheckAdzoneClickVariance.class);
        job.setReducerClass(CheckAdzoneClickVarianceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<61;i++) {
            CommUtil.addInputFileCommSpecialJob(job, fs, bidLogFile+"/log_date="+currentDate,
                    TextInputFormat.class, CheckAdzoneClickVarianceMapper.class, "");
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
