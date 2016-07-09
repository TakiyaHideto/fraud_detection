package com.mr.crossplatform.backup;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/2/29.
 */
public class ExtractBaiduCpDataForSVM {
    public static class ReadCpIpDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String baiduCpDataPath = context.getConfiguration().get("baiduCpDataPath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,baiduCpDataPath,this.cookieMobileMap,this.mobileCookieMap);
        }

        public static void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] deviceInfo = line.split(Properties.Base.BS_SEPARATOR_TAB);
                String mobile = deviceInfo[0].split(Properties.Base.BS_SEPARATOR,-1)[0];
                String cookie = deviceInfo[1].split(Properties.Base.BS_SUB_SEPARATOR,-1)[0];
                cookieMobileMap.put(cookie,mobile);
                mobileCookieMap.put(mobile,cookie);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("ReadCpIpDataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ReadCpIpDataMapper", "validOutputCt");

            validInputCt.increment(1);

            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String device = elementInfo[0];
            String deviceType = elementInfo[2];
            String[] ipBehaviour = elementInfo[3].split(Properties.Base.CTRL_B,-1);

            String cookie = "";
            String mobile = "";

            HashSet<String> ipMobileSet = new HashSet<String>();
            HashSet<String> ipCookieSet = new HashSet<String>();

            if ((deviceType.equals("2") || deviceType.equals("3")) &&
                    (this.mobileCookieMap.containsKey(device))){
                mobile = device;
                cookie = this.mobileCookieMap.get(device);
            } else if (deviceType.equals("1") &&
                    this.cookieMobileMap.containsKey(device)){
                cookie = device;
                mobile = this.cookieMobileMap.get(device);
            } else {
                return;
            }

            if (deviceType.equals("2") || deviceType.equals("3")){
                for (String ipInfo: ipBehaviour) {
                    String ip = ipInfo.split(Properties.Base.CTRL_C,-1)[0];
                    ipMobileSet.add(ip);
                }
            } else if (deviceType.equals("1")){
                for (String ipInfo: ipBehaviour) {
                    String ip = ipInfo.split(Properties.Base.CTRL_C,-1)[0];
                    ipCookieSet.add(ip);
                }
            } else {
                return;
            }

            if (!ipCookieSet.isEmpty()) {
                for (String ip : ipCookieSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(ip + Properties.Base.BS_SEPARATOR_UNDERLINE + "cookieIp");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            } else if (!ipMobileSet.isEmpty()) {
                for (String ip : ipMobileSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(ip + Properties.Base.BS_SEPARATOR_UNDERLINE + "mobileIp");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            }
        }
    }

    public static class ReadCpDomainDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String baiduCpDataPath = context.getConfiguration().get("baiduCpDataPath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,baiduCpDataPath,this.cookieMobileMap,this.mobileCookieMap);
        }

        public static void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] deviceInfo = line.split(Properties.Base.BS_SEPARATOR_TAB);
                String mobile = deviceInfo[0].split(Properties.Base.BS_SEPARATOR,-1)[0];
                String cookie = deviceInfo[1].split(Properties.Base.BS_SUB_SEPARATOR,-1)[0];
                cookieMobileMap.put(cookie,mobile);
                mobileCookieMap.put(mobile,cookie);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("ReadCpDomainDataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ReadCpDomainDataMapper", "validOutputCt");

            validInputCt.increment(1);

            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String device = elementInfo[0];
            String deviceType = elementInfo[2];
            String[] domainBehavior = elementInfo[3].split(Properties.Base.CTRL_B,-1);

            String cookie = "";
            String mobile = "";

            HashSet<String> domainMobileSet = new HashSet<String>();
            HashSet<String> domainCookieSet = new HashSet<String>();

            if (deviceType.equals("2") || deviceType.equals("3")){
                if (this.mobileCookieMap.containsKey(device)){
                    mobile = device;
                    cookie = this.mobileCookieMap.get(device);
                } else
                    return;
            } else if (deviceType.equals("1")){
                if (this.cookieMobileMap.containsKey(device)){
                    cookie = device;
                    mobile = this.cookieMobileMap.get(device);
                } else
                    return;
            } else {
                return;
            }

            if (deviceType.equals("2") || deviceType.equals("3")){
                for (String domainInfo: domainBehavior) {
                    String domain = domainInfo.split(Properties.Base.CTRL_C,-1)[0];
                    domainMobileSet.add(domain);
                }
            } else if (deviceType.equals("1")){
                for (String domainInfo: domainBehavior) {
                    String domain = domainInfo.split(Properties.Base.CTRL_C,-1)[0];
                    domainCookieSet.add(domain);
                }
            } else {
                return;
            }

            if (!domainCookieSet.isEmpty()) {
                for (String domain : domainCookieSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(domain + Properties.Base.BS_SEPARATOR_UNDERLINE + "cookieDomain");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            } else if (!domainMobileSet.isEmpty()) {
                for (String domain : domainMobileSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(domain + Properties.Base.BS_SEPARATOR_UNDERLINE + "mobileDomain");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            }
        }
    }

    public static class ReadCpAreaDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String baiduCpDataPath = context.getConfiguration().get("baiduCpDataPath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,baiduCpDataPath,this.cookieMobileMap,this.mobileCookieMap);
        }

        public static void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] deviceInfo = line.split(Properties.Base.BS_SEPARATOR_TAB);
                String mobile = deviceInfo[0].split(Properties.Base.BS_SEPARATOR,-1)[0];
                String cookie = deviceInfo[1].split(Properties.Base.BS_SUB_SEPARATOR,-1)[0];
                cookieMobileMap.put(cookie,mobile);
                mobileCookieMap.put(mobile,cookie);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("ReadCpAreaDataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ReadCpAreaDataMapper", "validOutputCt");

            validInputCt.increment(1);

            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String device = elementInfo[0];
            String deviceType = elementInfo[2];
            String[] areaBehavior = elementInfo[3].split(Properties.Base.CTRL_B,-1);

            String cookie = "";
            String mobile = "";

            HashSet<String> areaMobileSet = new HashSet<String>();
            HashSet<String> areaCookieSet = new HashSet<String>();

            if (deviceType.equals("2") || deviceType.equals("3")){
                if (this.mobileCookieMap.containsKey(device)){
                    mobile = device;
                    cookie = this.mobileCookieMap.get(device);
                } else
                    return;
            } else if (deviceType.equals("1")){
                if (this.cookieMobileMap.containsKey(device)){
                    cookie = device;
                    mobile = this.cookieMobileMap.get(device);
                } else
                    return;
            } else {
                return;
            }

            if (deviceType.equals("2") || deviceType.equals("3")){
                for (String areaInfo: areaBehavior) {
                    String area = areaInfo.split(Properties.Base.CTRL_C,-1)[0];
                    areaMobileSet.add(area);
                }
            } else if (deviceType.equals("1")){
                for (String areaInfo: areaBehavior) {
                    String area = areaInfo.split(Properties.Base.CTRL_C,-1)[0];
                    areaCookieSet.add(area);
                }
            } else {
                return;
            }

            if (!areaCookieSet.isEmpty()) {
                for (String area : areaCookieSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(area + Properties.Base.BS_SEPARATOR_UNDERLINE + "cookieArea");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            } else if (!areaMobileSet.isEmpty()) {
                for (String area : areaMobileSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(area + Properties.Base.BS_SEPARATOR_UNDERLINE + "mobileArea");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            }
        }
    }

    public static class ReadCpCrowdTagDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String baiduCpDataPath = context.getConfiguration().get("baiduCpDataPath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,baiduCpDataPath,this.cookieMobileMap,this.mobileCookieMap);
        }

        public static void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] deviceInfo = line.split(Properties.Base.BS_SEPARATOR_TAB);
                String mobile = deviceInfo[0].split(Properties.Base.BS_SEPARATOR,-1)[0];
                String cookie = deviceInfo[1].split(Properties.Base.BS_SUB_SEPARATOR,-1)[0];
                cookieMobileMap.put(cookie,mobile);
                mobileCookieMap.put(mobile,cookie);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("ReadCpCrowdTagDataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("ReadCpCrowdTagDataMapper", "validOutputCt");

            String line = value.toString();
            String[] elementInfo = line.split(Properties.Base.CTRL_A,-1);
            String device = elementInfo[0];
            String deviceType = elementInfo[2];
            String[] crowdTagBehavior = elementInfo[3].
                    split(Properties.Base.CTRL_B,-1)[0].
                    split(Properties.Base.CTRL_C,-1)[1].
                    split(Properties.Base.CTRL_D,-1);

            String cookie = "";
            String mobile = "";

            HashSet<String> crowdTagMobileSet = new HashSet<String>();
            HashSet<String> crowdTagCookieSet = new HashSet<String>();

            if (line.split(Properties.Base.BS_SEPARATOR,-1).length!=5 ||
                    !elementInfo[3].startsWith("7"))
                return;

            validInputCt.increment(1);

            if (deviceType.equals("2") || deviceType.equals("3")){
                if (this.mobileCookieMap.containsKey(device)){
                    mobile = device;
                    cookie = this.mobileCookieMap.get(device);
                } else
                    return;
            } else if (deviceType.equals("1")){
                if (this.cookieMobileMap.containsKey(device)){
                    cookie = device;
                    mobile = this.cookieMobileMap.get(device);
                } else
                    return;
            } else {
                return;
            }

            if (deviceType.equals("2") || deviceType.equals("3")){
                for (String crowdTagInfo: crowdTagBehavior) {
                    String crowdTag = crowdTagInfo;
                    crowdTagMobileSet.add(crowdTag);
                }
            } else if (deviceType.equals("1")){
                for (String crowdTagInfo: crowdTagBehavior) {
                    String crowdTag = crowdTagInfo;
                    crowdTagCookieSet.add(crowdTag);
                }
            } else {
                return;
            }

            if (!crowdTagCookieSet.isEmpty()) {
                for (String crowdTag : crowdTagCookieSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(crowdTag + Properties.Base.BS_SEPARATOR_UNDERLINE + "cookieCrowdTag");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            } else if (!crowdTagMobileSet.isEmpty()) {
                for (String crowdTag : crowdTagMobileSet) {
                    key_result.set(cookie + Properties.Base.BS_SEPARATOR_UNDERLINE + mobile);
                    value_result.set(crowdTag + Properties.Base.BS_SEPARATOR_UNDERLINE + "mobileCrowdTag");
                    validOutputCt.increment(1);
                    context.write(key_result,value_result);
                }
            }
        }
    }

    public static class ExtractBaiduCpDataForSVMReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            Counter validInputCt = context.getCounter("ExtractBaiduCpDataForSVMReducer","validInputCt");
            Counter validOutputCt = context.getCounter("ExtractBaiduCpDataForSVMReducer","validOutputCt");

            validInputCt.increment(1);

            HashSet<String> domainMobileSet = new HashSet<String>();
            HashSet<String> domainCookieSet = new HashSet<String>();
            HashSet<String> ipMobileSet = new HashSet<String>();
            HashSet<String> ipCookieSet = new HashSet<String>();
            HashSet<String> areaMobileSet = new HashSet<String>();
            HashSet<String> areaCookieSet = new HashSet<String>();
            HashSet<String> crowdTagMobileSet = new HashSet<String>();
            HashSet<String> crowdTagCookieSet = new HashSet<String>();

            HashSet<String> operationResultSet = new HashSet<String>();

            double jaccardCoefficientDomain = 0.0;
            int totalDomainNum = 0;
            int mobileDomainNum = 0;
            int cookieDomainNum = 0;
            int intersectionDomainNum = 0;
            int unionDomainNum = 0;

            double jaccardCoefficientIp = 0.0;
            int totalIpNum = 0;
            int mobileIpNum = 0;
            int cookieIpNum = 0;
            int intersectionIpNum = 0;
            int unionIpNum = 0;

            double jaccardCoefficientArea = 0.0;
            int totalAreaNum = 0;
            int mobileAreaNum = 0;
            int cookieAreaNum = 0;
            int intersectionAreaNum = 0;
            int unionAreaNum = 0;

            double jaccardCoefficientTag = 0.0;
            int totalTagNum = 0;
            int mobileTagNum = 0;
            int cookieTagNum = 0;
            int intersectionTagNum = 0;
            int unionTagNum = 0;

            for (Text value: values){
                String valueString = value.toString();
                String info = valueString.split(Properties.Base.BS_SEPARATOR_UNDERLINE,-1)[0];
                String deviceType = valueString.split(Properties.Base.BS_SEPARATOR_UNDERLINE,-1)[1];
                if (deviceType.equals("cookieIp")){
                    ipCookieSet.add(info);
                } else if (deviceType.equals("mobileIp")){
                    ipMobileSet.add(info);
                } else if (deviceType.equals("cookieDomain")){
                    domainCookieSet.add(info);
                } else if (deviceType.equals("mobileDomain")){
                    domainMobileSet.add(info);
                } else if (deviceType.equals("cookieArea")){
                    areaCookieSet.add(info);
                } else if (deviceType.equals("mobileArea")){
                    areaMobileSet.add(info);
                } else if (deviceType.equals("cookieCrowdTag")){
                    crowdTagCookieSet.add(info);
                } else if (deviceType.equals("mobileCrowdTag")){
                    crowdTagMobileSet.add(info);
                }
            }

            mobileDomainNum = domainMobileSet.size();
            cookieDomainNum = domainCookieSet.size();
            if (mobileDomainNum==0 || cookieDomainNum==0)
                return;
            totalDomainNum = mobileDomainNum + cookieDomainNum;
            operationResultSet.clear();
            operationResultSet.addAll(domainMobileSet);
            operationResultSet.retainAll(domainCookieSet);
            intersectionDomainNum = operationResultSet.size();
            operationResultSet.clear();;
            operationResultSet.addAll(domainMobileSet);
            operationResultSet.addAll(domainCookieSet);
            unionDomainNum = operationResultSet.size();
            jaccardCoefficientDomain = (double)intersectionDomainNum/(double)unionDomainNum;

            mobileAreaNum = areaMobileSet.size();
            cookieAreaNum = areaCookieSet.size();
            if (mobileAreaNum==0 || cookieAreaNum==0)
                return;
            totalAreaNum = mobileAreaNum + cookieAreaNum;
            operationResultSet.clear();
            operationResultSet.addAll(areaMobileSet);
            operationResultSet.retainAll(areaCookieSet);
            intersectionAreaNum = operationResultSet.size();
            operationResultSet.clear();
            operationResultSet.addAll(areaMobileSet);
            operationResultSet.addAll(areaCookieSet);
            unionAreaNum = operationResultSet.size();
            jaccardCoefficientArea = (double)intersectionAreaNum/(double)unionAreaNum;

            mobileIpNum = ipMobileSet.size();
            cookieIpNum = ipCookieSet.size();
            if (mobileIpNum==0 || cookieIpNum==0)
                return;
            totalAreaNum = mobileIpNum + cookieIpNum;
            operationResultSet.clear();
            operationResultSet.addAll(ipMobileSet);
            operationResultSet.retainAll(ipCookieSet);
            intersectionIpNum = operationResultSet.size();
            operationResultSet.clear();
            operationResultSet.addAll(ipMobileSet);
            operationResultSet.addAll(ipCookieSet);
            unionIpNum = operationResultSet.size();
            jaccardCoefficientIp = (double)intersectionIpNum/(double)unionIpNum;

            mobileTagNum = crowdTagMobileSet.size();
            cookieTagNum = crowdTagCookieSet.size();
            if (mobileTagNum==0 || cookieTagNum==0)
                return;
            totalDomainNum = mobileTagNum + cookieTagNum;
            operationResultSet.clear();
            operationResultSet.addAll(crowdTagMobileSet);
            operationResultSet.retainAll(crowdTagCookieSet);
            intersectionTagNum = operationResultSet.size();
            operationResultSet.clear();;
            operationResultSet.addAll(crowdTagMobileSet);
            operationResultSet.addAll(crowdTagCookieSet);
            unionTagNum = operationResultSet.size();
            jaccardCoefficientTag = (double)intersectionTagNum/(double)unionTagNum;

            value_result.set("1" +
                    Properties.Base.BS_SEPARATOR_SPACE + "1:" + String.valueOf(mobileAreaNum) +
                    Properties.Base.BS_SEPARATOR_SPACE + "2:" + String.valueOf(cookieAreaNum) +
                    Properties.Base.BS_SEPARATOR_SPACE + "3:" + String.valueOf(jaccardCoefficientDomain) +
                    Properties.Base.BS_SEPARATOR_SPACE + "4:" + String.valueOf(jaccardCoefficientArea) +
                    Properties.Base.BS_SEPARATOR_SPACE + "5:" + String.valueOf(jaccardCoefficientIp) +
                    Properties.Base.BS_SEPARATOR_SPACE + "6:" + String.valueOf(jaccardCoefficientTag));
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"ExtractBaiduCpDataForSVM");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(ExtractBaiduCpDataForSVM.class);
        job.setReducerClass(ExtractBaiduCpDataForSVMReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidMidLogPath = otherArgs[1];
        String baiduCpDataPath = otherArgs[2];
        String currentData = otherArgs[3];
        job.getConfiguration().set("baiduCpDataPath",baiduCpDataPath);

//        for (int i=0;i<=20;i++) {
        CommUtil.addInputFileComm(job, fs,
                bidMidLogPath + "/WebDomain",
                TextInputFormat.class, ReadCpDomainDataMapper.class);
        CommUtil.addInputFileComm(job, fs,
                bidMidLogPath + "/CrowdTag",
                TextInputFormat.class, ReadCpCrowdTagDataMapper.class);
        CommUtil.addInputFileComm(job, fs,
                bidMidLogPath + "/IP",
                TextInputFormat.class, ReadCpIpDataMapper.class);
        CommUtil.addInputFileComm(job, fs,
                bidMidLogPath + "/AreaCode",
                TextInputFormat.class, ReadCpAreaDataMapper.class);
        currentData = DateUtil.getSpecifiedDayBefore(currentData);
//        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
