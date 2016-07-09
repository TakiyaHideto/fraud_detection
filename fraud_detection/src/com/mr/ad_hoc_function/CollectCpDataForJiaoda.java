package com.mr.ad_hoc_function;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/10.
 */
public class CollectCpDataForJiaoda {
    public static class CollectCpDataForJiaodaMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileTrueMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieTrueMap = new HashMap<String, String>();
        private HashMap<String,String> cookieMobileFalseMap = new HashMap<String, String>();
        private HashMap<String,String> mobileCookieFalseMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String truePath = context.getConfiguration().get("truePath");
            String falsePath = context.getConfiguration().get("falsePath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,truePath,this.cookieMobileTrueMap,this.mobileCookieTrueMap);
            loadCacheIpArea(fs,falsePath,this.cookieMobileFalseMap,this.mobileCookieFalseMap);
        }

        public static void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] device = line.split(Properties.Base.CTRL_A);
                String mobile = device[0];
                String cookie = device[1];
                cookieMobileMap.put(cookie,mobile);
                mobileCookieMap.put(mobile,cookie);
//                addInfoToSet(cookieMobileMap,cookie,mobile);
//                addInfoToSet(mobileCookieMap,mobile,cookie);
            }
            br.close();
        }

//        public static void addInfoToSet(HashMap<String,String> map,String key,String value){
//            if (map.containsKey(key)){
//                HashSet<String> temp = map.get(key);
//                temp.add(value);
//                map.remove(key);
//                map.put(key,temp);
//            } else {
//                HashSet<String> temp = new HashSet<String>();
//                temp.add(value);
//                map.put(key,temp);
//            }
//        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("CollectCpDataCookieMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataCookieMapper", "validOutputCt");

            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

            String deviceId = elements[0]; // including pc and mobile
            String dataType = elements[1];
            String deviceType = elements[2];
            String dataInfo = elements[3];

            validInputCt.increment(1);

            // 8:ip
            if (dataType.equals("8")){
                String[] ipInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                if (deviceType.equals("1")) {
                    cookie = deviceId;
                    if (this.cookieMobileTrueMap.containsKey(cookie)) {
                        device = this.cookieMobileTrueMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.cookieMobileFalseMap.containsKey(cookie)) {
                        device = this.cookieMobileFalseMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                } else {
                    device = deviceId;
                    if (this.mobileCookieTrueMap.containsKey(device)) {
                        cookie = this.mobileCookieTrueMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.mobileCookieFalseMap.containsKey(device)) {
                        cookie = this.mobileCookieFalseMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }

            // 1:areacode
            else if (dataType.equals("1")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")) {
                    cookie = deviceId;
                    if (this.cookieMobileTrueMap.containsKey(cookie)) {
                        device = this.cookieMobileTrueMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.cookieMobileFalseMap.containsKey(cookie)) {
                        device = this.cookieMobileFalseMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                } else {
                    device = deviceId;
                    if (this.mobileCookieTrueMap.containsKey(device)) {
                        cookie = this.mobileCookieTrueMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.mobileCookieFalseMap.containsKey(device)) {
                        cookie = this.mobileCookieFalseMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }

            // 2:domain - pc
            else if (dataType.equals("2")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileTrueMap.containsKey(cookie)) {
                        device = this.cookieMobileTrueMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.cookieMobileFalseMap.containsKey(cookie)) {
                        device = this.cookieMobileFalseMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }

            // 3:webType - pc
            else if (dataType.equals("3")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileTrueMap.containsKey(cookie)) {
                        device = this.cookieMobileTrueMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.cookieMobileFalseMap.containsKey(cookie)) {
                        device = this.cookieMobileFalseMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }

            // 4:appType - mobile
            else if (dataType.equals("5")){
                String device = "";
                String cookie = "";
                if (!deviceType.equals("1")) {
                    device = deviceId;
                    if (this.mobileCookieTrueMap.containsKey(device)) {
                        cookie = this.mobileCookieTrueMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.mobileCookieFalseMap.containsKey(device)) {
                        cookie = this.mobileCookieFalseMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "mobile");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }

            // 5:crowdTag - pc
            else if (dataType.equals("0")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileTrueMap.containsKey(cookie)) {
                        device = this.cookieMobileTrueMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "trueCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                    if (this.cookieMobileFalseMap.containsKey(cookie)) {
                        device = this.cookieMobileFalseMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device +
                                Properties.Base.CTRL_A + "falseCp");
                        value_result.set(deviceId +
                                Properties.Base.CTRL_A + dataType +
                                Properties.Base.CTRL_A + deviceType +
                                Properties.Base.CTRL_A + dataInfo +
                                Properties.Base.CTRL_A + "cookie");
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            }
        }
    }

    public static class CollectCpDataForJiaodaReducer extends Reducer<Text, Text, NullWritable, Text> {
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

            Counter validInputCt = context.getCounter("CollectCpDataReducer","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataReducer", "validOutputCt");

            HashSet<String> ipSet = new HashSet<String>();
            HashSet<String> areaCodeSet = new HashSet<String>();
            HashSet<String> domainSet = new HashSet<String>();
            HashSet<String> webTypeSet = new HashSet<String>();
            HashSet<String> appTypeSet = new HashSet<String>();
            HashSet<String> crowdTagSet = new HashSet<String>();
            boolean hasCookieData = false;
            boolean hasMobileData = false;

            for (Text value: values){
                if (value.toString().contains("cookie")){
                    hasCookieData = true;
                } else if (value.toString().contains("mobile")){
                    hasMobileData = true;
                }
                String[] valueString = value.toString().split(Properties.Base.CTRL_A,-1);
                String dataType = valueString[1];
                if (dataType.equals("1")){
                    areaCodeSet.add(value.toString());
                } else if (dataType.equals("2")) {
                    domainSet.add(value.toString());
                } else if (dataType.equals("3")) {
                    webTypeSet.add(value.toString());
                } else if (dataType.equals("5")) {
                    appTypeSet.add(value.toString());
                } else if (dataType.equals("0")) {
                    crowdTagSet.add(value.toString());
                } else if (dataType.equals("8")) {
                    ipSet.add(value.toString());
                }
            }

            if (hasCookieData && hasMobileData){
                if (Math.random()>0.1){
                    value_result.set(key.toString());
                    mos.write("trainCp",NullWritable.get(),value_result,"trainCp");
                } else {
                    value_result.set(key.toString());
                    mos.write("testCp",NullWritable.get(),value_result,"testCp");
                }
                for (String value: ipSet){
                    value_result.set(value);
                    mos.write("ip",NullWritable.get(),value_result,"ip");
                    validOutputCt.increment(1);
                }
                for (String value: areaCodeSet){
                    value_result.set(value);
                    mos.write("areaCode",NullWritable.get(),value_result,"areaCode");
                    validOutputCt.increment(1);
                }
                for (String value: domainSet){
                    value_result.set(value);
                    mos.write("domain",NullWritable.get(),value_result,"domain");
                    validOutputCt.increment(1);
                }
                for (String value: webTypeSet){
                    value_result.set(value);
                    mos.write("webType",NullWritable.get(),value_result,"webType");
                    validOutputCt.increment(1);
                }
                for (String value: appTypeSet){
                    value_result.set(value);
                    mos.write("appType",NullWritable.get(),value_result,"appType");
                    validOutputCt.increment(1);
                }
                for (String value: crowdTagSet){
                    value_result.set(value);
                    mos.write("crowdTag",NullWritable.get(),value_result,"crowdTag");
                    validOutputCt.increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"CollectCpDataForJiaoda");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectCpDataForJiaoda.class);
        job.setReducerClass(CollectCpDataForJiaodaReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(15);

        conf.set("mapreduce.map.memory.mb ", "2000");
        conf.set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");
        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        MultipleOutputs.addNamedOutput(job,"crowdTag",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"areaCode",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"domain",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"webType",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"appType",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"ip",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"trainCp",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"testCp",TextOutputFormat.class,NullWritable.class, Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPath = otherArgs[1];
        String truePath = otherArgs[2];
        String falsePath = otherArgs[3];
        String currentData = otherArgs[4];

        job.getConfiguration().set("truePath",truePath);
        job.getConfiguration().set("falsePath",falsePath);

        for (int i=0;i<15;i++) {
            CommUtil.addInputFileComm(job, fs,
                    basedataPath + "/" + currentData + "/IP", TextInputFormat.class, CollectCpDataForJiaodaMapper.class);
            CommUtil.addInputFileCommSpecialJob(job,fs,
                    basedataPath + "/" + currentData + "/CrowdTag", TextInputFormat.class, CollectCpDataForJiaodaMapper.class, "pc");
            CommUtil.addInputFileComm(job, fs,
                    basedataPath + "/" + currentData + "/AreaCode", TextInputFormat.class, CollectCpDataForJiaodaMapper.class);
            CommUtil.addInputFileCommSpecialJob(job, fs,
                    basedataPath + "/" + currentData + "/WebDomain", TextInputFormat.class, CollectCpDataForJiaodaMapper.class, "pc");
            CommUtil.addInputFileCommSpecialJob(job, fs,
                    basedataPath + "/" + currentData + "/WebType", TextInputFormat.class, CollectCpDataForJiaodaMapper.class, "pc");
            CommUtil.addInputFileCommSpecialJob(job, fs,
                    basedataPath + "/" + currentData + "/AppType", TextInputFormat.class, CollectCpDataForJiaodaMapper.class, "idfa");
            CommUtil.addInputFileCommSpecialJob(job,fs,
                    basedataPath + "/" + currentData + "/AppType", TextInputFormat.class, CollectCpDataForJiaodaMapper.class, "imei");

            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
