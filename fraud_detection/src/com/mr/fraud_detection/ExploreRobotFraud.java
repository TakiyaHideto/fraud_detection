package com.mr.fraud_detection;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.MathUtil;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/4.
 */
public class ExploreRobotFraud {
    public static class ExploreRobotFraudMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("ExploreRobotFraudMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ExploreRobotFraudMapper", "validOutputCt");

            validInputCt.increment(1);
            // initialization
            // 提取基本信息feature
            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A, -1);
            String clk = elementInfo[2];
            String domain = elementInfo[22];
            String adzoneId = elementInfo[27];
            String logDate = elementInfo[31];
            String userAgent = elementInfo[35];
            String browser = elementInfo[36];
            String os = elementInfo[37];
            String ip = elementInfo[40];
            String yoyiCookie = elementInfo[42];

            try {
                // 提取IP局域网段
                String[] temp = ip.split("\\.",-1);
                String ipField = temp[0] + "." + temp[1] + "." + temp[2];

                key_result.set(ipField + Properties.Base.CTRL_A + logDate);
                value_result.set(clk +
                        Properties.Base.CTRL_A + domain +
                        Properties.Base.CTRL_A + adzoneId +
                        Properties.Base.CTRL_A + logDate +
                        Properties.Base.CTRL_A + userAgent +
                        Properties.Base.CTRL_A + browser +
                        Properties.Base.CTRL_A + os +
                        Properties.Base.CTRL_A + ip +
                        Properties.Base.CTRL_A + yoyiCookie);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class ExploreRobotFraudReducer extends Reducer<Text, Text, NullWritable, Text> {
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

            // initialize Counter
            Counter validOutputCt = context.getCounter("ExploreRobotFraudReducer", "validOutputCt");
            Counter ipFieldDateCt = context.getCounter("ExploreRobotFraudReducer","ipFieldDateCt");
            ipFieldDateCt.increment(1);

            String clk = "";
            String domain = "";
            String adzoneId = "";
            String logDate = "";
            String userAgent = "";
            String browser = "";
            String os = "";
            String ip = "";
            String yoyiCookie = "";
            String ipField = "";

            int impression = 0;

            HashMap<String,Integer> ipImpMap = new HashMap<String,Integer>();
            HashMap<String,Integer> ipClkMap = new HashMap<String,Integer>();
            HashMap<String,HashSet<String>> ipDomainMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipAdzoneIdMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipLogDateMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipUserAgentMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipBrowserMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipOsMap = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> ipYoyiCookieMap = new HashMap<String,HashSet<String>>();
            HashSet<String> ipSet = new HashSet<String>();
            HashSet<String> jaccardDomainIntstSet = new HashSet<String>();
            HashSet<String> jaccardAdzoneIdIntstSet = new HashSet<String>();
            HashSet<String> jaccardBrowserIntstSet = new HashSet<String>();
            HashSet<String> jaccardUserAgentIntstSet = new HashSet<String>();
            HashSet<String> jaccardCookieIntstSet = new HashSet<String>();
            HashSet<String> jaccardDomainUnionSet = new HashSet<String>();
            HashSet<String> jaccardAdzoneIdUnionSet = new HashSet<String>();
            HashSet<String> jaccardBrowserUnionSet = new HashSet<String>();
            HashSet<String> jaccardUserAgentUnionSet = new HashSet<String>();
            HashSet<String> jaccardCookieUnionSet = new HashSet<String>();

            int intersectionDomainNum = 0;
            int intersectionAdzoneIdNum = 0;
            int intersectionBrowserNum = 0;
            int intersectionUserAgentNum = 0;
            int intersectionCookieNum = 0;
            int unionDomainNum = 0;
            int unionAdzoneIdNum = 0;
            int unionBrowserNum = 0;
            int unionUserAgentNum = 0;
            int unionCookieNum = 0;
            double jaccardDomain = 0.0;
            double jaccardAdzoneId = 0.0;
            double jaccardBrowser = 0.0;
            double jaccardUserAgent = 0.0;
            double jaccardCookie = 0.0;

            for (Text value: values){
                String valueString = value.toString();
                String[] elementsInfo = valueString.split(Properties.Base.CTRL_A,-1);
                clk = elementsInfo[0];
                domain = elementsInfo[1];
                adzoneId = elementsInfo[2];
                logDate = elementsInfo[3];
                userAgent = elementsInfo[4];
                browser = elementsInfo[5];
                os = elementsInfo[6];
                ip = elementsInfo[7];
                yoyiCookie = elementsInfo[8];
                ipField = key.toString().split(Properties.Base.CTRL_A,-1)[0];

                if (clk.equals("1")){
                    this.addInfoToMapInteger(ipClkMap,ip);
                }
                this.addInfoToMapInteger(ipImpMap,ip);
                if (!ipClkMap.containsKey(ip)){
                    ipClkMap.put(ip,0);
                }
                this.addInfoToMapSet(ipDomainMap,ip,domain);
                this.addInfoToMapSet(ipAdzoneIdMap, ip, adzoneId);
                this.addInfoToMapSet(ipLogDateMap,ip,logDate);
                this.addInfoToMapSet(ipUserAgentMap,ip,userAgent);
                this.addInfoToMapSet(ipBrowserMap,ip,browser);
                this.addInfoToMapSet(ipOsMap,ip,os);
                this.addInfoToMapSet(ipYoyiCookieMap,ip,yoyiCookie);
                ipSet.add(ip);
                impression++;
            }

            HashSet<String> ipRemovedSet = new HashSet<String>();
            for (String ipInfo: ipImpMap.keySet()){
                if (ipClkMap.get(ipInfo)<=10 || ipImpMap.get(ipInfo)<1000)
                    ipRemovedSet.add(ipInfo);
            }
            for (String ipInfo: ipRemovedSet){
                ipImpMap.remove(ipInfo);
                if (ipClkMap.containsKey(ipInfo))
                    ipClkMap.remove(ipInfo);
                ipDomainMap.remove(ipInfo);
                ipAdzoneIdMap.remove(ipInfo);
                ipLogDateMap.remove(ipInfo);
                ipUserAgentMap.remove(ipInfo);
                ipBrowserMap.remove(ipInfo);
                ipOsMap.remove(ipInfo);
                ipYoyiCookieMap.remove(ipInfo);
                ipSet.remove(ipInfo);
            }
            if (ipImpMap.size()==0)
                return;

            int count = 0;
            jaccardBrowserIntstSet.clear();
            jaccardDomainIntstSet.clear();
            jaccardAdzoneIdIntstSet.clear();
            jaccardUserAgentIntstSet.clear();
            jaccardCookieIntstSet.clear();
            jaccardBrowserUnionSet.clear();
            jaccardDomainUnionSet.clear();
            jaccardAdzoneIdUnionSet.clear();
            jaccardUserAgentUnionSet.clear();
            jaccardCookieUnionSet.clear();
            for (String ipInfo: ipSet) {
                if (count==0) {
                    jaccardBrowserIntstSet.addAll(ipBrowserMap.get(ipInfo));
                    jaccardDomainIntstSet.addAll(ipDomainMap.get(ipInfo));
                    jaccardAdzoneIdIntstSet.addAll(ipAdzoneIdMap.get(ipInfo));
                    jaccardCookieIntstSet.addAll(ipYoyiCookieMap.get(ipInfo));
                    jaccardUserAgentIntstSet.addAll(ipUserAgentMap.get(ipInfo));
                } else {
                    jaccardBrowserIntstSet.retainAll(ipBrowserMap.get(ipInfo));
                    jaccardDomainIntstSet.retainAll(ipDomainMap.get(ipInfo));
                    jaccardAdzoneIdIntstSet.retainAll(ipAdzoneIdMap.get(ipInfo));
                    jaccardCookieIntstSet.retainAll(ipYoyiCookieMap.get(ipInfo));
                    jaccardUserAgentIntstSet.retainAll(ipUserAgentMap.get(ipInfo));
                }
                jaccardBrowserUnionSet.addAll(ipBrowserMap.get(ipInfo));
                jaccardDomainUnionSet.addAll(ipDomainMap.get(ipInfo));
                jaccardAdzoneIdUnionSet.addAll(ipAdzoneIdMap.get(ipInfo));
                jaccardCookieUnionSet.addAll(ipYoyiCookieMap.get(ipInfo));
                jaccardUserAgentUnionSet.addAll(ipUserAgentMap.get(ipInfo));
                count ++;
            }
            intersectionDomainNum = jaccardDomainIntstSet.size();
            intersectionAdzoneIdNum = jaccardAdzoneIdIntstSet.size();
            intersectionBrowserNum = jaccardBrowserIntstSet.size();
            intersectionUserAgentNum = jaccardUserAgentIntstSet.size();
            intersectionCookieNum = jaccardCookieIntstSet.size();
            unionDomainNum = jaccardDomainUnionSet.size();
            unionAdzoneIdNum = jaccardAdzoneIdUnionSet.size();
            unionBrowserNum = jaccardBrowserUnionSet.size();
            unionUserAgentNum = jaccardUserAgentUnionSet.size();
            unionCookieNum = jaccardCookieUnionSet.size();
            jaccardDomain = (double)intersectionDomainNum/(double)unionDomainNum;
            jaccardAdzoneId = (double)intersectionAdzoneIdNum/(double)unionAdzoneIdNum;
            jaccardBrowser = (double)intersectionBrowserNum/(double)unionBrowserNum;
            jaccardUserAgent = (double)intersectionUserAgentNum/(double)unionUserAgentNum;
            jaccardCookie = (double)intersectionCookieNum/(double)unionCookieNum;

            for (String ipInfo: ipSet) {
                int uaCount = ipUserAgentMap.get(ipInfo).size();
                int browserCount = ipBrowserMap.get(ipInfo).size();
                int osCount = ipOsMap.get(ipInfo).size();
                int yoyiCookieCount = ipYoyiCookieMap.get(ipInfo).size();
                int domainCount = ipDomainMap.get(ipInfo).size();
                int adzoneIdCount = ipAdzoneIdMap.get(ipInfo).size();
                double meanIpImp = MathUtil.calMeanMapInteger(ipImpMap);
                double sdIpImp = MathUtil.calStandardDeviationMapInteger(ipImpMap, meanIpImp);
                double meanIpClk = MathUtil.calMeanMapInteger(ipClkMap);
                double sdIpClk = MathUtil.calStandardDeviationMapInteger(ipClkMap, meanIpClk);
                int impressionNum = ipImpMap.get(ipInfo);
                int clickNum = (ipClkMap.containsKey(ipInfo))?ipClkMap.get(ipInfo):0;
                double ctr = (double)clickNum/(double)impressionNum;
                String domainList = StringUtil.listToString(jaccardDomainIntstSet,Properties.Base.CTRL_A);

                value_result.set(ipInfo +
                        Properties.Base.BS_SEPARATOR_TAB + logDate +
                        Properties.Base.BS_SEPARATOR_TAB + ipField +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(uaCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(browserCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(osCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(yoyiCookieCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(domainCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(adzoneIdCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(browserCount) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(jaccardCookie) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(jaccardDomain) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(jaccardAdzoneId) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(jaccardBrowser) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(jaccardUserAgent) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(impressionNum) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(clickNum) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(ctr) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(meanIpImp) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sdIpImp) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(meanIpClk) +
                        Properties.Base.BS_SEPARATOR_TAB + String.valueOf(sdIpClk));
                context.write(NullWritable.get(), value_result);
                value_result.set(domainList);
                mos.write("domainList",NullWritable.get(),value_result,"domainList");
                validOutputCt.increment(1);
            }
        }

        public static void addInfoToMapInteger(HashMap<String,Integer> map, String keyInfo){
            if (map.containsKey(keyInfo)){
                int count = map.get(keyInfo);
                map.remove(keyInfo);
                map.put(keyInfo,count+1);
            } else {
                map.put(keyInfo,1);
            }
        }

        public static void addInfoToMapSet(HashMap<String,HashSet<String>> map, String keyInfo, String valueInfo){
            HashSet<String> set = new HashSet<String>();
            if (map.containsKey(keyInfo)){
                set = map.get(keyInfo);
                set.add(valueInfo);
                map.remove(keyInfo);
                map.put(keyInfo,set);
            } else {
                set.add(valueInfo);
                map.put(keyInfo,set);
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

        Job job = new Job(conf,"ExploreRobotFraud");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        MultipleOutputs.addNamedOutput(job,"domainList",TextOutputFormat.class,NullWritable.class,Text.class);

        job.setJarByClass(ExploreRobotFraud.class);
        job.setReducerClass(ExploreRobotFraudReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);
        job.getConfiguration().set("mapreduce.reduce.memory.mb ", "4000");
        job.getConfiguration().set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx4000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedata = otherArgs[1];
        String currentDate = otherArgs[2];

//        for (int i=0;i<7;i++) {
//            CommUtil.addInputFileComm(job, fs, basedata+"/log_date="+currentDate, TextInputFormat.class, ExploreRobotFraudMapper.class);
//            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
//        }
        CommUtil.addInputFileComm(job, fs, basedata, TextInputFormat.class, ExploreRobotFraudMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
