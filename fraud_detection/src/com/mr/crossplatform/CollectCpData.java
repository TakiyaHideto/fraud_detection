package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
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
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/14.
 */
public class CollectCpData {
    public static class CollectCpDataCookieMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> trueCookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> trueMobileCookieMap = new HashMap<String, String>();
        private HashMap<String,String> falseCookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> falseMobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String truePath = context.getConfiguration().get("truePath");
            String falsePath = context.getConfiguration().get("falsePath");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,truePath,this.trueCookieMobileMap,this.trueMobileCookieMap);
            loadCacheIpArea(fs,falsePath,this.falseCookieMobileMap,this.falseMobileCookieMap);
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

            Counter tempCt = context.getCounter("CollectCpDataCookieMapper","tempCt");

            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

//            if (Math.random()>0.01)
//                return;

            String deviceId = elements[0]; // including pc and mobile
            String dataType = elements[1];
            String deviceType = elements[2];

            // 8:ip
            if (dataType.equals("8")){
                String[] ipInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                for (String ipInfoEle: ipInfo){
                    String ip = ipInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String timestamp = ipInfoEle.split(Properties.Base.CTRL_C,-1)[1];
                    String date = "";
                    String hour = "";
                    try {
                        date = DateUtil.getTimeOfDate(Long.parseLong(timestamp));
                        hour = String.valueOf(DateUtil.getTimeOfHour(Long.parseLong(timestamp)));

//                        String frequency = ipInfoEle.split(Properties.Base.CTRL_C, -1)[1];
                        String frequency = "1";
                        if (deviceType.equals("1")) {
                            cookie = deviceId;
                            if (this.trueCookieMobileMap.containsKey(cookie)) {
                                device = this.trueCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "trueCp");
                                value_result.set(ip +
                                        Properties.Base.CTRL_A + frequency +
                                        Properties.Base.CTRL_A + date +
                                        Properties.Base.CTRL_A + hour +
                                        Properties.Base.CTRL_A + "ip_cookie");
                                context.write(key_result, value_result);
                            }
                            if (this.falseCookieMobileMap.containsKey(cookie)) {
                                device = this.falseCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "falseCp");
                                value_result.set(ip +
                                        Properties.Base.CTRL_A + frequency +
                                        Properties.Base.CTRL_A + date +
                                        Properties.Base.CTRL_A + hour +
                                        Properties.Base.CTRL_A + "ip_cookie");
                                context.write(key_result, value_result);
                            }
                        } else {
                            device = deviceId;
                            if (this.trueMobileCookieMap.containsKey(device)) {
                                cookie = this.trueMobileCookieMap.get(device);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "trueCp");
                                value_result.set(ip +
                                        Properties.Base.CTRL_A + frequency +
                                        Properties.Base.CTRL_A + date +
                                        Properties.Base.CTRL_A + hour +
                                        Properties.Base.CTRL_A + "ip_mobile");
                                context.write(key_result, value_result);
                            }
                            if (this.falseMobileCookieMap.containsKey(device)) {
                                cookie = this.falseMobileCookieMap.get(device);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "falseCp");
                                value_result.set(ip +
                                        Properties.Base.CTRL_A + frequency +
                                        Properties.Base.CTRL_A + date +
                                        Properties.Base.CTRL_A + hour +
                                        Properties.Base.CTRL_A + "ip_mobile");
                                context.write(key_result, value_result);
                            }
                        }
                    } catch (Exception e){
                        continue;
                    }
                }
            }

            // 1:areacode - mobile
            else if (dataType.equals("1")){
                String[] areaCodeInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String device = "";
                String cookie = "";
                for (String areaCodeInfoEle: areaCodeInfo){
                    String areaCode = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String frequency = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[1];
                    if (!deviceType.equals("1")) {
                        device = deviceId;
                        if (this.trueMobileCookieMap.containsKey(device)) {
                            cookie = trueMobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_mobile");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                            context.write(key_result, value_result);
                        }
                        if (this.falseMobileCookieMap.containsKey(device)){
                            cookie = falseMobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "falseCp");
                            value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_mobile");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                            context.write(key_result, value_result);
                        }
                    } else {
                        tempCt.increment(1);
                        cookie = deviceId;
                        if (this.trueCookieMobileMap.containsKey(cookie)) {
                            device = trueCookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_cookie");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                            context.write(key_result, value_result);
                        }
                        if (this.falseCookieMobileMap.containsKey(cookie)){
                            device = falseCookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "falseCp");
                            value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_cookie");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                            context.write(key_result, value_result);
                        }
                    }
                }
            }

            // 2:domain - pc
            else if (dataType.equals("2")){
                String[] domainInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                for (String domainInfoEle: domainInfo){
                    String domain = domainInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String frequency = domainInfoEle.split(Properties.Base.CTRL_C,-1)[1];
                    if (deviceType.equals("1")){
                        cookie = deviceId;
                        if (this.trueCookieMobileMap.containsKey(cookie)) {
                           device = trueCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "trueCp");
                                value_result.set(domain + ":" + frequency + Properties.Base.CTRL_A + "domain");
                                context.write(key_result, value_result);

                        }
                        if (this.falseCookieMobileMap.containsKey(cookie)){
                            device = falseCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "falseCp");
                                value_result.set(domain + ":" + frequency + Properties.Base.CTRL_A + "domain");
                                context.write(key_result, value_result);
                        }
                    }
                }
            }

            // 3:webType - pc
            else if (dataType.equals("3")){
                String[] webTypeInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String cookie = "";
                String device = "";
                for (String webTypeInfoEle: webTypeInfo){
                    String webType = webTypeInfoEle.split(Properties.Base.CTRL_C,-1)[0].split(Properties.Base.CTRL_D)[1];
                    String frequency = webTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1];
                    if (deviceType.equals("1")){
                        cookie = deviceId;
                        if (this.trueCookieMobileMap.containsKey(cookie)) {
                            device = trueCookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(webType + ":" + frequency + Properties.Base.CTRL_A + "webType");
//                            value_result.set(webType + ":1" + Properties.Base.CTRL_A + "webType");
                            context.write(key_result, value_result);
                        }
                        if (this.falseCookieMobileMap.containsKey(cookie)){
                           device = falseCookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "falseCp");
                            value_result.set(webType + ":" + frequency + Properties.Base.CTRL_A + "webType");
//                            value_result.set(webType + ":1" + Properties.Base.CTRL_A + "webType");
                            context.write(key_result, value_result);
                        }
                    }
                }
            }

            // 4:appType - mobile
            else if (dataType.equals("5")){
                String[] appTypeInfo = elements[3].split(Properties.Base.CTRL_B, -1);
                String device = "";
                String cookie = "";
                for (String appTypeInfoEle: appTypeInfo){
                    String appType = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[0].split(Properties.Base.CTRL_D)[1];
                    String frequency = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1];
                    if (!deviceId.equals("1")) {
                        device = deviceId;
                        if (this.trueMobileCookieMap.containsKey(device)) {
                            cookie = trueMobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(appType + ":" + frequency + Properties.Base.CTRL_A + "appType");
//                            value_result.set(appType + ":1" + Properties.Base.CTRL_A + "appType");
                            context.write(key_result, value_result);
                        }
                        if (this.falseMobileCookieMap.containsKey(device)){
                            cookie = this.falseMobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "falseCp");
                            value_result.set(appType + ":" + frequency + Properties.Base.CTRL_A + "appType");
//                            value_result.set(appType + ":1" + Properties.Base.CTRL_A + "appType");
                            context.write(key_result, value_result);
                        }
                    }
                }
            }

            // 4:crowdTag - pc
            else if (dataType.equals("0")){
                String[] crowdTagInfo = elements[3].split(Properties.Base.CTRL_B,-1);
                String cookie = "";
                String device = "";
                for (String crowdTagInfoEle: crowdTagInfo){
                    String[] crowdTag = crowdTagInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    for (String tag: crowdTag) {
                        if (deviceType.equals("1")) {
                            cookie = deviceId;
                            if (this.trueCookieMobileMap.containsKey(cookie)) {
                                device = trueCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "trueCp");
                                value_result.set(tag + ":1" + Properties.Base.CTRL_A + "crowdTag");
                                context.write(key_result, value_result);
                            }
                            if (this.falseCookieMobileMap.containsKey(cookie)) {
                                device = falseCookieMobileMap.get(cookie);
                                key_result.set(cookie +
                                        Properties.Base.CTRL_A + device +
                                        Properties.Base.CTRL_A + "falseCp");
                                value_result.set(tag + ":1" + Properties.Base.CTRL_A + "crowdTag");
                                context.write(key_result, value_result);

                            }
                        }
                    }
                }
            }

        }
    }

    public static class CollectCpDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            Counter validInputCt = context.getCounter("CollectCpDataReducer","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataReducer", "validOutputCt");
            Counter tempCt = context.getCounter("CollectCpDataReducer","tempCt");

            HashSet<String> ipCookieSet = new HashSet<String>();
            HashSet<String> ipMobileSet = new HashSet<String >();
            HashSet<String> cookieIpDateSet = new HashSet<String >();
            HashSet<String> mobileIpDateSet = new HashSet<String >();
            HashSet<String> cookieAreaCodeDateSet = new HashSet<String >();
            HashSet<String> mobileAreaCodeDateSet = new HashSet<String >();

            HashMap<String,Integer> areaCodeMap = new HashMap<String,Integer>();
            HashMap<String,Integer> domainMap = new HashMap<String,Integer>();
            HashMap<String,Integer> webTypeMap = new HashMap<String,Integer>();
            HashMap<String,Integer> appTypeMap = new HashMap<String,Integer>();
            HashMap<String,Integer> crowdTagMap = new HashMap<String,Integer>();

            HashMap<String,HashSet<String>> mobileHourSet = new HashMap<String,HashSet<String>>();
            HashMap<String,HashSet<String>> cookieHourSet = new HashMap<String,HashSet<String>>();
            boolean hasCookieData = false;
            boolean hasMobileData = false;
            int cookieBidCt = 0;
            int mobileBidCt = 0;

            for (Text value: values){
                String valueString = value.toString();
                if (valueString.contains("ip")){
                    if (valueString.contains("_cookie")) {
                        String[] ipInfo = valueString.split(Properties.Base.CTRL_A,-1);
                        ipCookieSet.add(ipInfo[0]);
                        cookieBidCt += Integer.parseInt(ipInfo[1]);
                        cookieIpDateSet.add(ipInfo[2]);
                        CommUtil.addInfoToMapWithSet(cookieHourSet,ipInfo[2],ipInfo[3]);
                        hasCookieData = true;
                    }
                    else if (valueString.contains("_mobile")) {
                        String[] ipInfo = valueString.split(Properties.Base.CTRL_A,-1);
                        ipMobileSet.add(ipInfo[0]);
                        mobileBidCt += Integer.parseInt(ipInfo[1]);
                        mobileIpDateSet.add(ipInfo[2]);
                        CommUtil.addInfoToMapWithSet(mobileHourSet, ipInfo[2], ipInfo[3]);
                        hasMobileData = true;
                    }
                } else if (valueString.contains("areaCode")){
                    String areaCodeInfo = valueString.split(Properties.Base.CTRL_A)[0];
                    String device = valueString.split(Properties.Base.CTRL_A,-1)[1];
                    String areaCode = areaCodeInfo.split(":")[0];
                    int frequency = Integer.parseInt(areaCodeInfo.split(":")[1]);
                    CommUtil.countKeyFrquecyMap(areaCodeMap, areaCode+device, frequency);
                    if (valueString.contains("cookie")){
                        cookieAreaCodeDateSet.add(areaCode);
                        tempCt.increment(1);
                    } else if (valueString.contains("mobile")){
                        mobileAreaCodeDateSet.add(areaCode);
                    }
                } else if (valueString.contains("domain")){
                    String domainInfo = valueString.split(Properties.Base.CTRL_A)[0];
                    String domain = domainInfo.split(":")[0];
                    int frequency = Integer.parseInt(domainInfo.split(":")[1]);
                    CommUtil.countKeyFrquecyMap(domainMap, domain, frequency);
                } else if (valueString.contains("webType")){
                    String webTypeInfo = valueString.split(Properties.Base.CTRL_A)[0];
                    String webType = webTypeInfo.split(":")[0];
                    int frequency = Integer.parseInt(webTypeInfo.split(":")[1]);
                    CommUtil.countKeyFrquecyMap(webTypeMap, webType, frequency);
                } else if (valueString.contains("appType")){
                    String appTypeInfo = valueString.split(Properties.Base.CTRL_A)[0];
                    String appType = appTypeInfo.split(":")[0];
                    int frequency = Integer.parseInt(appTypeInfo.split(":")[1]);
                    CommUtil.countKeyFrquecyMap(appTypeMap, appType, frequency);
                } else if (valueString.contains("crowdTag")){
                    String crowdTagInfo = valueString.split(Properties.Base.CTRL_A)[0];
                    String crowdTag = crowdTagInfo.split(":")[0];
                    int frequency = Integer.parseInt(crowdTagInfo.split(":")[1]);
                    CommUtil.countKeyFrquecyMap(crowdTagMap, crowdTag, frequency);
                }
            }

            // 删除特定crowdTag
            HashSet<String> removeSet = new HashSet<String>();
            for (String crowdTag: crowdTagMap.keySet()){
                if (crowdTagMap.get(crowdTag)<5){
                    removeSet.add(crowdTag);
                }
            }
            for (String crowdTag: removeSet){
                crowdTagMap.remove(crowdTag);
            }

            String areaCodeString = StringUtil.mapToString(areaCodeMap, Properties.Base.BS_SEPARATOR_SPACE);
//            String domainString = StringUtil.mapToString(domainMap, Properties.Base.BS_SEPARATOR_SPACE);
            String webTypeString = StringUtil.mapToString(webTypeMap, Properties.Base.BS_SEPARATOR_SPACE);
            String appTypeString = StringUtil.mapToString(appTypeMap, Properties.Base.BS_SEPARATOR_SPACE);
//            String crowdTagString = StringUtil.mapToString(crowdTagMap, Properties.Base.BS_SEPARATOR_SPACE);

            if (appTypeString.equals("")){
//                hasMobileData = false;
                return;
            }
            if (webTypeString.equals("")){
//                hasCookieData = false;
                return;
            }

            // ip jaccard
            HashSet<String> operationSet = new HashSet<String>();
            operationSet.clear();
            operationSet.addAll(ipCookieSet);
            operationSet.retainAll(ipMobileSet);
            double intersection = (double)operationSet.size();
            operationSet.clear();
            operationSet.addAll(ipCookieSet);
            operationSet.addAll(ipMobileSet);
            double union = (double)operationSet.size();
            // areaCode jaccard
            operationSet.clear();
            operationSet.addAll(cookieAreaCodeDateSet);
            operationSet.retainAll(mobileAreaCodeDateSet);
            double intersectionArea = (double)operationSet.size();
            operationSet.clear();
            operationSet.addAll(cookieAreaCodeDateSet);
            operationSet.addAll(mobileAreaCodeDateSet);
            double unionArea = (double)operationSet.size();

            operationSet.clear();
            operationSet.addAll(mobileIpDateSet);
            operationSet.retainAll(cookieIpDateSet);
            double commonDate = (double)operationSet.size();

            double cookieHourMean = 0.0;
            for (String mapKey: cookieHourSet.keySet()){
                cookieHourMean += cookieHourSet.get(mapKey).size();
            }
            cookieHourMean = cookieHourMean/(double)cookieHourSet.size();
            double mobileHourMean = 0.0;
            for (String mapKey: mobileHourSet.keySet()){
                mobileHourMean += mobileHourSet.get(mapKey).size();
            }
            mobileHourMean = mobileHourMean/(double)mobileHourSet.size();

            if (hasCookieData && hasMobileData) {
                if (key.toString().contains("trueCp")) {
                    value_result.set("1" +
                            Properties.Base.BS_SEPARATOR_SPACE + "ipIntst:" + String.valueOf(intersection / union) +
                            Properties.Base.BS_SEPARATOR_SPACE + "areaIntst:" + String.valueOf(intersectionArea/unionArea) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieIpNum:" + String.valueOf(ipCookieSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileIpNum:" + String.valueOf(ipMobileSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "commonDate:" + String.valueOf(commonDate) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieDateNum:" + String.valueOf(cookieIpDateSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileDateNum:" + String.valueOf(mobileIpDateSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieHourMean:" + String.valueOf(cookieHourMean) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileHourMean:" + String.valueOf(mobileHourMean) +
                            Properties.Base.BS_SEPARATOR_SPACE + "IPMultiple:" + String.valueOf((double)ipMobileSet.size()/(double)ipCookieSet.size()) +
//                            Properties.Base.BS_SEPARATOR_SPACE + crowdTagString +
                            Properties.Base.BS_SEPARATOR_SPACE + areaCodeString +
                            Properties.Base.BS_SEPARATOR_SPACE + webTypeString +
//                            Properties.Base.BS_SEPARATOR_SPACE + domainString +
                            Properties.Base.BS_SEPARATOR_SPACE + appTypeString);
                    context.write(NullWritable.get(), value_result);
                } else if (key.toString().contains("falseCp")) {
                    value_result.set("0" +
                            Properties.Base.BS_SEPARATOR_SPACE + "ipIntst:" + String.valueOf(intersection / union) +
                            Properties.Base.BS_SEPARATOR_SPACE + "areaIntst:" + String.valueOf(intersectionArea/unionArea) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieIpNum:" + String.valueOf(ipCookieSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileIpNum:" + String.valueOf(ipMobileSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "commonDate:" + String.valueOf(commonDate) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieDateNum:" + String.valueOf(cookieIpDateSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileDateNum:" + String.valueOf(mobileIpDateSet.size()) +
                            Properties.Base.BS_SEPARATOR_SPACE + "cookieHourMean:" + String.valueOf(cookieHourMean) +
                            Properties.Base.BS_SEPARATOR_SPACE + "mobileHourMean:" + String.valueOf(mobileHourMean) +
                            Properties.Base.BS_SEPARATOR_SPACE + "IPMultiple:" + String.valueOf((double)ipMobileSet.size()/(double)ipCookieSet.size()) +
//                            Properties.Base.BS_SEPARATOR_SPACE + crowdTagString +
                            Properties.Base.BS_SEPARATOR_SPACE + areaCodeString +
                            Properties.Base.BS_SEPARATOR_SPACE + webTypeString +
//                            Properties.Base.BS_SEPARATOR_SPACE + domainString +
                            Properties.Base.BS_SEPARATOR_SPACE + appTypeString);
                    context.write(NullWritable.get(), value_result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"CollectCpData");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().set("mapreduce.map.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        job.getConfiguration().set("mapreduce.reduce.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectCpData.class);
        job.setReducerClass(CollectCpDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidMidLogPath = otherArgs[1];
        String truePath = otherArgs[2];
        String falsePath = otherArgs[3];
        String currentData = otherArgs[4];
        job.getConfiguration().set("truePath",truePath);
        job.getConfiguration().set("falsePath",falsePath);

        job.getConfiguration().set("mapreduce.map.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        job.getConfiguration().set("mapreduce.reduce.memory.mb ", "8000");
        job.getConfiguration().set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx8000000000");

        CommUtil.addInputFileComm(job, fs, bidMidLogPath, TextInputFormat.class, CollectCpDataCookieMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
