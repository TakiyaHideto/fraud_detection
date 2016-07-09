package com.mr.crossplatform;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/4/12.
 */
public class YoyiCpPart5CollectOriginData {
    public static class YoyiCpPart5CollectOriginDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashMap<String,String> cookieMobileMap = new HashMap<String,String>();
        private HashMap<String,String> mobileCookieMap = new HashMap<String,String>();
        private int deviceThre = 1;
        private FileSystem fs = null;

        public YoyiCpPart5CollectOriginDataMapper(){
            this.deviceThre = 1;
        }

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
                if (Math.random()>0.6){
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
            tempSet.addAll(Arrays.asList(list.split(Properties.Base.CTRL_A, -1)));
            return tempSet;
        }

        private String convertSetToString(HashSet<String> set){
            String temp = StringUtil.listToString(set, Properties.Base.CTRL_A);
            return temp;
        }


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
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        for (String deviceTemp: this.cookieMobileMap.get(cookie).split(Properties.Base.CTRL_A,-1)) {
//                            device = this.cookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + deviceTemp +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "cookie_ip");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                } else {
                    device = deviceId;
                    if (this.mobileCookieMap.containsKey(device)) {
                        for (String cookieTemp: this.mobileCookieMap.get(device).split(Properties.Base.CTRL_A, -1)) {
//                            cookie = this.mobileCookieMap.get(device);
                            key_result.set(cookieTemp +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "mobile_ip");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }

            // 1:areacode
            else if (dataType.equals("1")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")) {
                    cookie = deviceId;
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        for (String deviceTemp: cookieMobileMap.get(cookie).split(Properties.Base.CTRL_A, -1)) {
//                        device = this.cookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + deviceTemp +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "cookie_areacode");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                } else {
                    device = deviceId;
                    if (this.mobileCookieMap.containsKey(device)) {
                        for (String cookieTemp : this.mobileCookieMap.get(device).split(Properties.Base.CTRL_A, -1)) {
//                            cookie = this.mobileCookieMap.get(device);
                            key_result.set(cookieTemp +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "mobile_areacode");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }

            // 2:domain - pc
            else if (dataType.equals("2")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileMap.containsKey(cookie)) {
//                        device = this.cookieMobileMap.get(cookie);
                        for (String deviceTemp: cookieMobileMap.get(cookie).split(Properties.Base.CTRL_A, -1)) {
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + deviceTemp +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "cookie_domain");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }

            // 3:webType - pc
            else if (dataType.equals("3")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        for (String deviceTemp : this.cookieMobileMap.get(cookie).split(Properties.Base.CTRL_A, -1)) {
//                        device = this.cookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + deviceTemp +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "cookie_webType");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }

            // 4:appType - mobile
            else if (dataType.equals("5")){
                String device = "";
                String cookie = "";
                if (!deviceType.equals("1")) {
                    device = deviceId;
                    if (this.mobileCookieMap.containsKey(device)) {
                        for (String cookieTemp : this.mobileCookieMap.get(device).split(Properties.Base.CTRL_A, -1)) {
//                            cookie = this.mobileCookieMap.get(device);
                            key_result.set(cookieTemp +
                                    Properties.Base.CTRL_A + device +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "mobile_appType");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }

            // 5:crowdTag - pc
            else if (dataType.equals("0")){
                String device = "";
                String cookie = "";
                if (deviceType.equals("1")){
                    cookie = deviceId;
                    if (this.cookieMobileMap.containsKey(cookie)) {
                        for (String deviceTemp : this.cookieMobileMap.get(cookie).split(Properties.Base.CTRL_A, -1)) {
//                            device = this.cookieMobileMap.get(cookie);
                            key_result.set(cookie +
                                    Properties.Base.CTRL_A + deviceTemp +
                                    Properties.Base.CTRL_A + "trueCp");
                            value_result.set(line + Properties.Base.CTRL_A + "cookie_crowdTag");
                            context.write(key_result, value_result);
                            validOutputCt.increment(1);
                        }
                    }
                }
            }
        }
    }

    public static class YoyiCpPart5CollectOriginDataReducer extends Reducer<Text, Text, NullWritable, Text> {
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
            if (!treeList.isEmpty()){
                tempCt.increment(1);
            }
            for(HashMap<String,String> tree: this.treeList){
                String branch = tree.get("0");

                boolean flag = true;
                while (flag) {
                    HashMap<String, String> branchInfoMap = parseBranchInfo(branch);

                    if (branchInfoMap.containsKey("value")) {
                        linearValue += Double.parseDouble(branchInfoMap.get("value"));
                        flag = false;
                    } else {
                        String feature = this.featureIdMap.get(branchInfoMap.get("feature"));
                        for (String featureValue: featureList.split(Properties.Base.BS_SEPARATOR_SPACE, -1)) {
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
            value_result.set(key.toString() + Properties.Base.CTRL_A + String.valueOf(probability));
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
        }
    }

//    public static class YoyiCpPart5CollectOriginDataReducer extends Reducer<Text, Text, NullWritable, Text> {
//        private Text key_result = new Text();
//        private Text value_result = new Text();
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values,Context context)
//                throws IOException, InterruptedException {
//
//            Counter validInputCt = context.getCounter("CollectCpDataReducer","validInputCt");
//            Counter validOutputCt = context.getCounter("CollectCpDataReducer", "validOutputCt");
//
//            validInputCt.increment(1);
//
//            HashSet<String> dataSet = new HashSet<String>();
//
//            boolean hasCookieData = false;
//            boolean hasMobileData = false;
//
//            for (Text value: values){
//                if (value.toString().contains("cookie")){
//                    hasCookieData = true;
//                } else if (value.toString().contains("mobile")){
//                    hasMobileData = true;
//                }
//                dataSet.add(value.toString());
//            }
//
//            if (hasCookieData && hasMobileData){
//                for (String value: dataSet){
//                    value_result.set(value);
//                    context.write(NullWritable.get(),value_result);
//                    validOutputCt.increment(1);
//                }
//            }
//        }
//    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 6){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"YoyiCpPart5CollectOriginData");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(YoyiCpPart5CollectOriginData.class);
        job.setReducerClass(YoyiCpPart5CollectOriginDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(100);

        conf.set("mapreduce.map.memory.mb ", "4000");
        conf.set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx4000000000");
        conf.set("mapreduce.reduce.memory.mb ", "2000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPath = otherArgs[1];
        String deviceCoupleFile = otherArgs[2];
        String currentData = otherArgs[3];
        String dump = otherArgs[4];
        String featureIdFile = otherArgs[5];
        job.getConfiguration().set("dump",dump);
        job.getConfiguration().set("featureIdFile",featureIdFile);
        job.getConfiguration().set("deviceCoupleFile",deviceCoupleFile);

//        job.getConfiguration().set("deviceCoupleFile",deviceCoupleFile);

        CommUtil.addInputFileComm(job, fs, basedataPath, TextInputFormat.class, YoyiCpPart5CollectOriginDataMapper.class);

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
