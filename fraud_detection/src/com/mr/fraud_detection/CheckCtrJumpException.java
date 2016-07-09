package com.mr.fraud_detection;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.DateUtil;
import com.mr.utils.MathUtil;
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
 * Created by TakiyaHideto on 16/3/11.
 */
public class CheckCtrJumpException {
    public static class CheckCtrJumpExceptionMapper extends Mapper<Object, Text, Text, Text> {
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

            String click = elementInfo[2];
            String ip = elementInfo[40];
            String domain = elementInfo[22];
            String adzoneId = elementInfo[27];
            String algoData = elementInfo[7];
            String logDate = elementInfo[31];
            if (algoData.split(Properties.Base.CTRL_B,-1).length==0) {
                return;
            }
            try {
                String ctrBucket = algoData.split(Properties.Base.CTRL_B,-1)[0];
                if (!ctrBucket.equals("ID_lr_UA")
//                        || !ctrBucket.equals("lr_fm")
//                        || !ctrBucket.equals("FM")
                        )
                    return;
                String ctrPredicted = algoData.split(Properties.Base.CTRL_B, -1)[1];
                double ctrPrdField = Double.parseDouble(ctrPredicted);
                String ctrField = "";
                if (ctrPrdField > 0.0 && ctrPrdField < 0.001) {
                    key_result.set("0.0"+" "+"0.001" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.001 && ctrPrdField < 0.002) {
                    key_result.set("0.001"+" "+"0.002" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.002 && ctrPrdField < 0.003) {
                    key_result.set("0.002"+" "+"0.003" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.003 && ctrPrdField < 0.004) {
                    key_result.set("0.003"+" "+"0.004" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.004 && ctrPrdField < 0.005) {
                    key_result.set("0.004"+" "+"0.005" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.005 && ctrPrdField < 0.006) {
                    key_result.set("0.005"+" "+"0.006" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.006 && ctrPrdField < 0.007) {
                    key_result.set("0.006"+" "+"0.007" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.007 && ctrPrdField < 0.008) {
                    key_result.set("0.007"+" "+"0.008" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.008 && ctrPrdField < 0.009) {
                    key_result.set("0.008"+" "+"0.009" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.009 && ctrPrdField < 0.01) {
                    key_result.set("0.009"+" "+"0.01" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.01 && ctrPrdField < 0.011) {
                    key_result.set("0.01"+" "+"0.011" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.011 && ctrPrdField < 0.012) {
                    key_result.set("0.011"+" "+"0.012" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.012 && ctrPrdField < 0.013) {
                    key_result.set("0.012"+" "+"0.013" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.013 && ctrPrdField < 0.014) {
                    key_result.set("0.013"+" "+"0.014" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.014 && ctrPrdField < 0.015) {
                    key_result.set("0.014"+" "+"0.015" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.015 && ctrPrdField < 0.016) {
                    key_result.set("0.015"+" "+"0.016" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.016 && ctrPrdField < 0.017) {
                    key_result.set("0.016"+" "+"0.017" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.017 && ctrPrdField < 0.018) {
                    key_result.set("0.017"+" "+"0.018" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.018 && ctrPrdField < 0.019) {
                    key_result.set("0.018"+" "+"0.019" + Properties.Base.CTRL_A + logDate);
                }
                else if (ctrPrdField >= 0.019 && ctrPrdField < 0.02) {
                    key_result.set("0.019"+" "+"0.02" + Properties.Base.CTRL_A + logDate);
                }
                else{
                    return;
                }

                // output key-value
                value_result.set(click +
                        Properties.Base.CTRL_A + ip +
                        Properties.Base.CTRL_A + domain +
                        Properties.Base.CTRL_A + adzoneId);
                context.write(key_result, value_result);
                validOutputCt.increment(1);
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class CheckCtrJumpExceptionReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CheckAdzoneClickVarianceReducer", "validOutputCt");

            HashMap<String,Integer> impMap = new HashMap<String,Integer>();
            HashMap<String,Integer> clkMap = new HashMap<String,Integer>();
            LinkedHashMap<String,Integer> ipClkMap = new LinkedHashMap<String,Integer>();
            HashSet<String> ipSet= new HashSet<String>();
            LinkedHashMap<String,Integer> domainMap = new LinkedHashMap<String,Integer>();
            LinkedHashMap<String,Integer> adzoneMap = new LinkedHashMap<String,Integer>();

            String ctrField = key.toString().split(Properties.Base.CTRL_A,-1)[0];
            String ctrUpper = ctrField.split(" ",-1)[1];

            int clk = 0;
            int imp = 0;
            for (Text value: values){
                String[] valueEle = value.toString().split(Properties.Base.CTRL_A,-1);
                if (valueEle[0].equals("1")){
//                    addInfoToMap(clkMap,ctrField);
                    clk++;
                    imp++;
                    addInfoToMap(ipClkMap,valueEle[1]);
                    ipSet.add(valueEle[1]);
                    addInfoToMap(domainMap,valueEle[2]);
                    addInfoToMap(adzoneMap,valueEle[3]);
                } else if (valueEle[0].equals("0")){
//                    addInfoToMap(clkMap,ctrField);
//                    addInfoToMap(impMap,ctrField);
                    imp++;
                }
            }

            LinkedHashMap<String,Integer> ipLink = MathUtil.sortMap(ipClkMap);
            LinkedHashMap<String,Integer> domainLink = MathUtil.sortMap(domainMap);
            LinkedHashMap<String,Integer> adzoneLink = MathUtil.sortMap(adzoneMap);
            int ipClkedTotal = ipLink.size();
            int ipClkNum = MathUtil.calSumInteger(ipLink);
            double ipRatioCumulative = 1 - (double)MathUtil.calRatioCumulative(ipLink, 0.5, ipClkNum)/(double)ipClkedTotal;

            int domainClkedTotal = domainLink.size();
            int domainClkNum = MathUtil.calSumInteger(domainLink);
            double domainRatioCumulative = 1 - (double)MathUtil.calRatioCumulative(domainLink, 0.5, domainClkNum)/(double)domainClkedTotal;

            int adzoneClkedTotal = adzoneLink.size();
            int adzoneClkNum = MathUtil.calSumInteger(adzoneLink);
            double adzoneRatioCumulative = 1 - (double)MathUtil.calRatioCumulative(adzoneLink, 0.5, adzoneClkNum)/(double)adzoneClkedTotal;

//            if (ipRatioCumulative!=0.0){
//                return;
//            }

            String ip = "";
            Iterator it1 = ipLink.entrySet().iterator();
            while(it1.hasNext()){
                Map.Entry entry = (Map.Entry) it1.next();
                System.out.println(entry.getKey() + ":" + entry.getValue());
                ip = String.valueOf(entry.getKey());
            }
            String domain = "";
            Iterator it2 = domainLink.entrySet().iterator();
            while(it2.hasNext()){
                Map.Entry entry = (Map.Entry) it2.next();
                System.out.println(entry.getKey() + ":" + entry.getValue());
                domain = String.valueOf(entry.getKey());
            }
            String adzone = "";
            Iterator it3 = adzoneLink.entrySet().iterator();
            while(it3.hasNext()){
                Map.Entry entry = (Map.Entry) it3.next();
                System.out.println(entry.getKey() + ":" + entry.getValue());
                adzone = String.valueOf(entry.getKey());
            }

            value_result.set(key.toString() +
                        Properties.Base.CTRL_A + String.valueOf(imp) +
                        Properties.Base.CTRL_A + String.valueOf(clk) +
                        Properties.Base.CTRL_A + String.valueOf((double)clk/(double)imp - Double.parseDouble(ctrUpper)) +
                        Properties.Base.CTRL_A + String.valueOf((double)clk/(double)imp) +
                        Properties.Base.CTRL_A + ipSet.size() +
                        Properties.Base.CTRL_A + domainMap.size() +
                        Properties.Base.CTRL_A + adzoneMap.size() +
                        Properties.Base.CTRL_A + String.valueOf(ipRatioCumulative) +
                        Properties.Base.CTRL_A + String.valueOf(domainRatioCumulative) +
                        Properties.Base.CTRL_A + String.valueOf(adzoneRatioCumulative) +
                        Properties.Base.CTRL_A + String.valueOf(ip) + ":" + ipLink.get(ip) +
                        Properties.Base.CTRL_A + String.valueOf(domain) + ":" + domainLink.get(domain) +
                        Properties.Base.CTRL_A + String.valueOf(adzone) + ":" + adzoneLink.get(adzone));

//            value_result.set(key.toString() +
//                    Properties.Base.CTRL_A + String.valueOf((double)clk/(double)imp - Double.parseDouble(ctrUpper)) +
//                    Properties.Base.CTRL_A + String.valueOf(ip));

            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);
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

        Job job = new Job(conf,"CheckCtrJumpException");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        conf.set("mapreduce.reduce.memory.mb ", "3000");
        conf.set("mapreduce.reduce.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx3000000000");

        job.setJarByClass(CheckCtrJumpException.class);
        job.setReducerClass(CheckCtrJumpExceptionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidLogFile = otherArgs[1];
        String currentDate = otherArgs[2];

        for (int i=0;i<1;i++) {
            CommUtil.addInputFileCommSpecialJob(job, fs, bidLogFile+"/log_date="+currentDate,
                    TextInputFormat.class, CheckCtrJumpExceptionMapper.class, "");
            currentDate = DateUtil.getSpecifiedDayBefore(currentDate);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
