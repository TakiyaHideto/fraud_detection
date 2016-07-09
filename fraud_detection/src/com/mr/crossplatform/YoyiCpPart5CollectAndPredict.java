package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.ModelUtil;
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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by hideto on 16/5/3.
 */
public class YoyiCpPart5CollectAndPredict {
    public static class YoyiCpPart5CollectAndPredictMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> CookieMobileMap = new HashMap<String, String>();
        private HashMap<String,String> MobileCookieMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String deviceCoupleFile = context.getConfiguration().get("deviceCoupleFile");
            fs = FileSystem.get(conf);
            loadCacheIpArea(fs,deviceCoupleFile,this.CookieMobileMap,this.MobileCookieMap);
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
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("CollectCpDataCookieMapper","validInputCt");
            Counter validOutputCt = context.getCounter("CollectCpDataCookieMapper", "validOutputCt");

            Counter tempCt = context.getCounter("CollectCpDataCookieMapper","tempCt");

            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

            String deviceId = elements[0];
            String deviceInfo = elements[1];
            String dataType = elements[2];

            // 8:ip
            if (dataType.contains("IP_LOG")){
                String[] ipInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String cookie = deviceId;
                String device = deviceId;
                for (String ipInfoEle: ipInfo){
                    String ip = ipInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = ipInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    try {
                        if (this.CookieMobileMap.containsKey(cookie)) {
                            cookie = deviceId;
                            device = this.CookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device);
                            value_result.set(ip +
                                    Properties.Base.CTRL_A + frequency +
                                    Properties.Base.CTRL_A + date +
                                    Properties.Base.CTRL_A + hour +
                                    Properties.Base.CTRL_A + "ip_cookie");
                            context.write(key_result, value_result);
                        }
                        if (this.MobileCookieMap.containsKey(device)) {
                            cookie = this.MobileCookieMap.get(device);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + device);
                            value_result.set(ip +
                                    Properties.Base.CTRL_A + frequency +
                                    Properties.Base.CTRL_A + date +
                                    Properties.Base.CTRL_A + hour +
                                    Properties.Base.CTRL_A + "ip_mobile");
                            context.write(key_result, value_result);
                        }
                    } catch (Exception e){
                        continue;
                    }
                }
            }

            // 1:areacode
            else if (dataType.contains("AREACODE_LOG")){
                String[] areaCodeInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String device = deviceId;
                String cookie = deviceId;
                for (String areaCodeInfoEle: areaCodeInfo){
                    String areaCode = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = areaCodeInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.MobileCookieMap.containsKey(device)) {
                        device = deviceId;
                        cookie = MobileCookieMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_mobile");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                        context.write(key_result, value_result);
                    }
                    if (this.CookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = CookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(areaCode + ":" + frequency + Properties.Base.CTRL_A + "areaCode_cookie");
//                            value_result.set(areaCode + ":1" + Properties.Base.CTRL_A + "areaCode");
                        context.write(key_result, value_result);
                    }
                }
            }

            // 2:domain - pc
            else if (dataType.contains("DOMAIN_LOG")){
                String[] domainInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String cookie = deviceId;
                String device = deviceId;
                for (String domainInfoEle: domainInfo){
                    String domain = domainInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = domainInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.CookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = CookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(domain + ":" + frequency + Properties.Base.CTRL_A + "domain");
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
            else if (dataType.contains("APPTYPE_LOG")){
                String[] appTypeInfo = deviceInfo.split(Properties.Base.CTRL_B, -1);
                String device = deviceId;
                String cookie = deviceId;
                for (String appTypeInfoEle: appTypeInfo){
                    String appType = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = appTypeInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.MobileCookieMap.containsKey(device)) {
                        device = deviceId;
                        cookie = MobileCookieMap.get(device);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(appType + ":" + frequency + Properties.Base.CTRL_A + "appType");
//                            value_result.set(appType + ":1" + Properties.Base.CTRL_A + "appType");
                        context.write(key_result, value_result);
                    }
                }
            }

            // 4:crowdTag - pc
            else if (dataType.contains("CROWDTAG_LOG")){
                String[] crowdTagInfo = deviceInfo.split(Properties.Base.CTRL_B,-1);
                String cookie = deviceId;
                String device = deviceId;
                for (String crowdTagInfoEle: crowdTagInfo){
                    String crowdTag = crowdTagInfoEle.split(Properties.Base.CTRL_C,-1)[0];
                    String[] timeInfo = crowdTagInfoEle.split(Properties.Base.CTRL_C,-1)[1].split(Properties.Base.CTRL_D,-1);
                    String date = timeInfo[0];
                    String hour = timeInfo[1];
                    String frequency = timeInfo[2];
                    if (this.CookieMobileMap.containsKey(cookie)) {
                        cookie = deviceId;
                        device = CookieMobileMap.get(cookie);
                        key_result.set(cookie +
                                Properties.Base.CTRL_A + device);
                        value_result.set(crowdTag + ":1" + Properties.Base.CTRL_A + "crowdTag");
                        context.write(key_result, value_result);
                    }
                }
            }

        }
    }

    public static class YoyiCpPart5CollectAndPredictReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> featureIdMap = new HashMap<String,String>();
        private LinkedList<HashMap<String,String>> treeList = new LinkedList<HashMap<String, String>>();

        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String featureIdFile = (String)context.getConfiguration().get("featureIdFile");
            String dump = (String)context.getConfiguration().get("dump");
            loadFeatureId(fs, featureIdFile, this.featureIdMap);
            this.treeList = loadTreeDumpFile(fs, dump, context);
        }

        public static void loadFeatureId(FileSystem fs, String path, HashMap<String,String> map)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String featureName = line.split(Properties.Base.BS_SEPARATOR_TAB,-1)[1];
                String featureId = line.split(Properties.Base.BS_SEPARATOR_TAB,-1)[0];
                map.put(featureId,featureName);
            }
            br.close();
        }

        public static LinkedList<HashMap<String,String>> loadTreeDumpFile(FileSystem fs, String path, Context context)
                throws FileNotFoundException,IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            LinkedList<HashMap<String,String>> treeList = new LinkedList<HashMap<String, String>>();
            String line;
            HashMap<String, String> dumpMap = new HashMap<String, String>();
            int nextTreeNum = 0;
            while ((line = br.readLine()) != null) {
                if (line.equals("booster[" + nextTreeNum + "]:")) {
                    if (nextTreeNum > 0) {
                        if(dumpMap.size()>0) {
                            context.getCounter("CollectYoyiCpDataAndPredictReducer","treeCt").increment(1);
                            HashMap<String,String> tempDumpMap = new HashMap<String,String>(dumpMap);
                            treeList.add(tempDumpMap);
                            dumpMap.clear();
                        }
                    }
                    nextTreeNum++;
                } else {
                    String[] branchInfo = line.trim().split(":", 2);
                    String index = branchInfo[0];
                    String info = branchInfo[1];
                    dumpMap.put(index, info);
                }
            }
            if (dumpMap.size()>0) {
                HashMap<String,String> tempDumpMap = new HashMap<String,String>(dumpMap);
                treeList.add(tempDumpMap);
                dumpMap.clear();
            }
            br.close();
            return treeList;
        }

        public static HashMap<String,String> parseBranchInfo(String branchInfo){

            HashMap<String, String> branchInfoMap = new HashMap<String, String>();

            if (branchInfo.contains("yes") && branchInfo.contains("no")) {
                String tmp = branchInfo.split("yes",-1)[0];
                String featureCond = tmp.substring(1, tmp.length() - 2);
                String feature = featureCond.split("<", -1)[0].replaceAll("f","");
                String condition = featureCond.split("<", -1)[1];
                String[] branch = branchInfo.split(Properties.Base.BS_SEPARATOR_SPACE, -1)[1].split(",", -1);
                String yesBrc = branch[0].split("=", -1)[1];
                String noBrc = branch[1].split("=", -1)[1];
                String missingBrc = branch[2].split("=", -1)[1];

                branchInfoMap.put("feature", feature);
                branchInfoMap.put("condition", condition);
                branchInfoMap.put("yes", yesBrc);
                branchInfoMap.put("no", noBrc);
                branchInfoMap.put("missing", missingBrc);
            } else if (branchInfo.contains("leaf=")){
                String value = branchInfo.split("=",-1)[1];
                branchInfoMap.put("value",value);
            }

            return branchInfoMap;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            if (key.toString().contains("null"))
                return;

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
                        CommUtil.addInfoToMapWithSet(cookieHourSet, ipInfo[2], ipInfo[3]);
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

//            if (appTypeString.equals("")){
////                hasMobileData = false;
//                return;
//            }
//            if (webTypeString.equals("")){
////                hasCookieData = false;
//                return;
//            }

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

            String featureList = "ipIntst:" + String.valueOf(intersection / union) +
                    Properties.Base.BS_SEPARATOR_SPACE + "areaIntst:" + String.valueOf(intersectionArea/unionArea) +
                    Properties.Base.BS_SEPARATOR_SPACE + "cookieIpNum:" + String.valueOf(ipCookieSet.size()) +
                    Properties.Base.BS_SEPARATOR_SPACE + "mobileIpNum:" + String.valueOf(ipMobileSet.size()) +
                    Properties.Base.BS_SEPARATOR_SPACE + "commonDate:" + String.valueOf(commonDate) +
                    Properties.Base.BS_SEPARATOR_SPACE + "cookieDateNum:" + String.valueOf(cookieIpDateSet.size()) +
                    Properties.Base.BS_SEPARATOR_SPACE + "mobileDateNum:" + String.valueOf(mobileIpDateSet.size()) +
                    Properties.Base.BS_SEPARATOR_SPACE + "cookieHourMean:" + String.valueOf(cookieHourMean) +
                    Properties.Base.BS_SEPARATOR_SPACE + "mobileHourMean:" + String.valueOf(mobileHourMean) +
                    Properties.Base.BS_SEPARATOR_SPACE + "IPMultiple:" + String.valueOf((double)ipMobileSet.size()/(double)ipCookieSet.size()) +
                    Properties.Base.BS_SEPARATOR_SPACE + areaCodeString +
                    Properties.Base.BS_SEPARATOR_SPACE + webTypeString +
                    Properties.Base.BS_SEPARATOR_SPACE + appTypeString;

            double linearValue = 0.0;
            Boolean isMissing = true;
            if (!this.treeList.isEmpty()){
                tempCt.increment(1);
            }
            for(HashMap<String,String> tree: this.treeList) {
                String branch = tree.get("0");
                boolean flag = true;
                while (flag) {
                    HashMap<String, String> branchInfoMap = parseBranchInfo(branch);

                    if (branchInfoMap.containsKey("value")) {
                        linearValue += Double.parseDouble(branchInfoMap.get("value"));
                        flag = false;
                    } else {
                        String feature = this.featureIdMap.get(branchInfoMap.get("feature"));
                        for (String featureValue : featureList.split(Properties.Base.BS_SEPARATOR_SPACE, -1)) {
                            if (featureValue.contains(feature)) {
                                if (Double.parseDouble(featureValue.split(":", -1)[1]) < Double.parseDouble(branchInfoMap.get("condition"))) {
                                    branch = tree.get(branchInfoMap.get("yes"));
                                    isMissing = false;
                                    break;
                                } else {
                                    branch = tree.get(branchInfoMap.get("no"));
                                    isMissing = false;
                                    break;
                                }
                            }
                        }
                        if (isMissing) {
                            branch = tree.get(branchInfoMap.get("missing"));
                        }
                        isMissing = true;
                    }
                }
            }
            double probability = ModelUtil.calSigmoidProbability(linearValue);
            if (probability<0.5)
                return;
            value_result.set(key.toString() + Properties.Base.CTRL_A + String.valueOf(probability));
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 6){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(2);
        }

        Job job = new Job(conf,"YoyiCpPart5CollectAndPredict");
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
        job.setJarByClass(YoyiCpPart5CollectAndPredict.class);
        job.setReducerClass(YoyiCpPart5CollectAndPredictReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidMidLogPath = otherArgs[1];
//        String bidMidlogAccuPath = otherArgs[2];
        String deviceCoupleFile = otherArgs[2];
        String dump = otherArgs[3];
        String featureIdFile = otherArgs[4];
        String currentData = otherArgs[5];
        job.getConfiguration().set("dump",dump);
        job.getConfiguration().set("featureIdFile",featureIdFile);
        job.getConfiguration().set("deviceCoupleFile",deviceCoupleFile);

        CommUtil.addInputFileComm(job, fs, bidMidLogPath, TextInputFormat.class, YoyiCpPart5CollectAndPredictMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
