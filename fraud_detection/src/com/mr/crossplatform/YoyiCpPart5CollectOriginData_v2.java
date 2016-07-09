package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.zip.*;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.net.URI;

/**
 * Created by TakiyaHideto on 16/4/6.
 */
public class YoyiCpPart5CollectOriginData_v2 {
    public static class YoyiCpPart5CollectOriginData_v2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String,String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String,String>();
        private int deviceThre = 1;
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String deviceCoupleFile = context.getConfiguration().get("deviceCoupleFile");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,deviceCoupleFile,this.cookieMobileMap,this.mobileCookieMap,context);
        }

        public void loadCacheIpArea(FileSystem fs,String path,
                                           HashMap<String,String> cookieMobileMap,
                                           HashMap<String,String> mobileCookieMap,
                                           Context context)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                if (Math.random()>0.2){
                    continue;
                }
                try {
                    String[] device = line.split(Properties.Base.CTRL_A, -1);
                    String mobile = device[0];
                    String cookie = device[1];
//                    this.addInfoToSet(cookieMobileMap, cookie, mobile);
//                    this.addInfoToSet(mobileCookieMap, mobile, cookie);
                    cookieMobileMap.put(cookie, mobile);
                    mobileCookieMap.put(mobile, cookie);
                } catch (ArrayIndexOutOfBoundsException e){
                    System.out.print(line);
                }
            }
            br.close();
        }

        private int getDeviceThre(){
            return this.deviceThre;
        }

        private void addInfoToSet(HashMap<String,String> map,String key,String value){
//            HashSet<String> tempSet = new HashSet<String>();
            if (map.containsKey(key)){
//                String temp = map.get(key);
//                HashSet<String> tempSet = new HashSet<String>(convertStringToSet(temp));
//                if (tempSet.size()>this.getDeviceThre()){
//                    tempSet.clear();
//                    return;
//                }
//                tempSet.add(value);
//                temp = convertSetToString(tempSet);
//                map.remove(key);
//                map.put(key,temp);
//                tempSet.clear();
                String temp = map.get(key);
                temp += Properties.Base.CTRL_A + value;
                if (temp.split(Properties.Base.CTRL_A,-1).length>1)
                    return;
                map.remove(key);
                map.put(key,temp);
            } else {
                map.put(key,value);
            }
        }

        private HashSet<String> convertStringToSet(String list){
            HashSet<String> tempSet = new HashSet<String>();
            tempSet.addAll(Arrays.asList(list.split(Properties.Base.CTRL_A,-1)));
            return tempSet;
        }

        private String convertSetToString(HashSet<String> set){
            String temp = StringUtil.listToString(set,Properties.Base.CTRL_A);
            return temp;
        }


        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("CollectCpDataCookieMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataCookieMapper", "validOutputCt");

            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

            String deviceId = elements[0];
            String deviceInfo = elements[1];
            String dataType = elements[2];

            // 8:ip
            if (dataType.equals("IP_LOG")){
                String[] ipInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                for (String ipInfoEle: ipInfo){
                    String ip = ipInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = ipInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    try {
                        if (this.cookieMobileMap.containsKey(cookie)) {
                            cookie = deviceId;
                            device = this.cookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device);
                            value_result.set(line +
                                    Properties.Base.CTRL_A + "ip_cookie");
                            context.write(key_result, value_result);
                        }
                        if (this.mobileCookieMap.containsKey(device)) {
                            cookie = this.mobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device);
                            value_result.set(line +
                                    Properties.Base.CTRL_A + "ip_mobile");
                            context.write(key_result, value_result);
                        }
                    } catch (Exception e){
                        continue;
                    }
                }
            }

            // 1:areacode
            else if (dataType.equals("AREACODE_LOG")){
                String[] areaCodeInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String device = "";
                String cookie = "";
                for (String areaCodeInfoEle: areaCodeInfo){
                    String areaCode = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.mobileCookieMap.containsKey(device)) {
                        device = deviceId;
                        cookie = mobileCookieMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(line + ":" + frequency + Properties.Base.CTRL_A + "areaCode_mobile");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                        context.write(key_result, value_result);
                    }
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = cookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(line + ":" + frequency + Properties.Base.CTRL_A + "areaCode_cookie");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                        context.write(key_result, value_result);
                    }
                }
            }

            // 2:domain - pc
            else if (dataType.equals("DOMAIN_LOG")){
                String[] domainInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                for (String domainInfoEle: domainInfo){
                    String domain = domainInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = domainInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = cookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(line + ":" + frequency + Properties.Base.CTRL_A + "domain");
                        context.write(key_result, value_result);

                    }
                }
            }

//            // 3:webType - pc
//            else if (dataType.equals("3")){
//                String[] webTypeInfo = elements[3].split(Properties.Base.CTRL_B, -1);
//                String cookie = "";
//                String device = "";
//                for (String webTypeInfoEle: webTypeInfo){
//                    String webType = webTypeInfoEle.split(Properties.Base.CTRL_C,-1)[0].split(Properties.Base.CTRL_D)[1];
//                    String[] timeInfo = webTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
//                    String date = timeInfo[0];
//                    String hour = timeInfo[1];
//                    String frequency = timeInfo[2];
//                    String frequency = webTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1];
//                    if (deviceType.equals("1")){
//                        cookie = deviceId;
//                        if (this.CookieMobileMap.containsKey(cookie)) {
//                            device = CookieMobileMap.get(cookie);
//                            key_result.set(cookie +
//                                    Properties.Base.CTRL_A + device);
//                            value_result.set(webType + ":" + frequency + Properties.Base.CTRL_A + "webType");
////                            value_result.set(webType + ":1" + Properties.Base.CTRL_A + "webType");
//                            context.write(key_result, value_result);
//                        }
//                    }
//                }
//            }

            // 4:appType - mobile
            else if (dataType.equals("APPTYPE_LOG")){
                String[] appTypeInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String device = "";
                String cookie = "";
                for (String appTypeInfoEle: appTypeInfo){
                    String appType = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.mobileCookieMap.containsKey(device)) {
                        device = deviceId;
                        cookie = mobileCookieMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(line + ":" + frequency + Properties.Base.CTRL_A + "appType");
//                            value_result.set(appType + ":1" + Properties.Base.CTRL_A + "appType");
                        context.write(key_result, value_result);
                    }
                }
            }

            // 4:crowdTag - pc
            else if (dataType.equals("CROWDTAG_LOG")){
                String[] crowdTagInfo = deviceInfo.split(Properties.Base.CTRL_B,-1);
                String cookie = "";
                String device = "";
                for (String crowdTagInfoEle: crowdTagInfo){
                    String crowdTag = crowdTagInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = crowdTagInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = cookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(line + ":1" + Properties.Base.CTRL_A + "crowdTag");
                        context.write(key_result, value_result);
                    }
                }
            }
        }
    }

    public static class YoyiCpPart5CollectOriginData_v2Reducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            Counter validInputCt = context.getCounter("CollectCpDataReducer","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataReducer", "validOutputCt");

            validInputCt.increment(1);

            HashSet<String> dataSet = new HashSet<String>();

            boolean hasCookieData = false;
            boolean hasMobileData = false;

            for (Text value: values){
                if (value.toString().contains("cookie")){
                    hasCookieData = true;
                } else if (value.toString().contains("mobile")){
                    hasMobileData = true;
                }
                dataSet.add(value.toString());
            }

            if (hasCookieData && hasMobileData){
                for (String value: dataSet){
                    value_result.set(value);
                    context.write(NullWritable.get(),value_result);
                    validOutputCt.increment(1);
                }
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

        Job job = new Job(conf,"YoyiCpPart5CollectOriginData_v2");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart5CollectOriginData_v2.class);
        job.setReducerClass(YoyiCpPart5CollectOriginData_v2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(100);

//        conf.set("mapreduce.map.memory.mb ", "2000");
//        conf.set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");
        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPath = otherArgs[1];
        String deviceCoupleFile = otherArgs[2];
        String currentData = otherArgs[3];

        job.getConfiguration().set("deviceCoupleFile",deviceCoupleFile);

        CommUtil.addInputFileComm(job, fs, basedataPath, TextInputFormat.class, YoyiCpPart5CollectOriginData_v2Mapper.class);

//        for (int i=0;i<15;i++) {
//            CommUtil.addInputFileComm(job, fs,
//                    basedataPath + "/" + currentData + "/IP", TextInputFormat.class, CollectCpOriginDataMapper.class);
//            currentData = DateUtil.getSpecifiedDayBefore(currentData);
//            CommUtil.addInputFileCommSpecialJob(job,fs,
//                    basedataPath + "/" + currentData + "/CrowdTag", TextInputFormat.class, CollectCpOriginDataMapper.class, "pc");
//            CommUtil.addInputFileComm(job, fs,
//                    basedataPath + "/" + currentData + "/AreaCode", TextInputFormat.class, CollectCpOriginDataMapper.class);
//            CommUtil.addInputFileCommSpecialJob(job, fs,
//                    basedataPath + "/" + currentData + "/WebDomain", TextInputFormat.class, CollectCpOriginDataMapper.class, "pc");
//            CommUtil.addInputFileCommSpecialJob(job, fs,
//                    basedataPath + "/" + currentData + "/WebType", TextInputFormat.class, CollectCpOriginDataMapper.class, "pc");
//            CommUtil.addInputFileCommSpecialJob(job, fs,
//                    basedataPath + "/" + currentData + "/AppType", TextInputFormat.class, CollectCpOriginDataMapper.class, "idfa");
//            CommUtil.addInputFileCommSpecialJob(job,fs,
//                    basedataPath + "/" + currentData + "/AppType", TextInputFormat.class, CollectCpOriginDataMapper.class, "imei");
//        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
