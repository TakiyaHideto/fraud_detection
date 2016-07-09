package com.mr.ad_hoc_function;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/8.
 */
public class CollectCpDataWithSpecificFeats {
    public static class pcDataMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> cookieSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String trueData = (String)context.getConfiguration().get("trueData");
            String falseData = (String)context.getConfiguration().get("falseData");
            readCookie(fs,trueData,this.cookieSet);
            readCookie(fs,falseData,this.cookieSet);
        }

        public static void readCookie(FileSystem fs, String path, HashSet<String> set)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                set.add(line.trim().split(Properties.Base.CTRL_A)[1]);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("AddFilterBlackListForBasedataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("AddFilterBlackListForBasedataMapper", "validOutputCt");


            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.CTRL_A, -1);

            validInputCt.increment(1);

            if (this.cookieSet.contains(elementsInfo[42])){
                String timestamp = elementsInfo[30];
                String ip = elementsInfo[40];
                String ipType = "";
                String ipGeo = "";
                String userAgent = elementsInfo[35];
                String geoId = elementsInfo[39];
                String postalCode = "";
                String userCluster = "";
                String url = elementsInfo[24];
                String contentTagList = elementsInfo[22];
                String mediaInfo = "";
                String browser = elementsInfo[36];
                String os = elementsInfo[37];
                String interest = elementsInfo[44];
                String interestCate = "";
                String gender = elementsInfo[43];

                value_result.set("pc" +
                        Properties.Base.CTRL_A + timestamp +
                        Properties.Base.CTRL_A + ip +
                        Properties.Base.CTRL_A + ipType +
                        Properties.Base.CTRL_A + ipGeo +
                        Properties.Base.CTRL_A + userAgent +
                        Properties.Base.CTRL_A + geoId +
                        Properties.Base.CTRL_A + postalCode +
                        Properties.Base.CTRL_A + userCluster +
                        Properties.Base.CTRL_A + url +
                        Properties.Base.CTRL_A + contentTagList +
                        Properties.Base.CTRL_A + mediaInfo +
                        Properties.Base.CTRL_A + browser +
                        Properties.Base.CTRL_A + os +
                        Properties.Base.CTRL_A + interest +
                        Properties.Base.CTRL_A + interestCate +
                        Properties.Base.CTRL_A + gender);
                context.write(NullWritable.get(),value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static class mobileDataMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> deviceSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String trueData = (String)context.getConfiguration().get("trueData");
            String falseData = (String)context.getConfiguration().get("falseData");
            readDevice(fs,trueData,this.deviceSet);
            readDevice(fs,falseData,this.deviceSet);
        }

        public static void readDevice(FileSystem fs, String path, HashSet<String> set)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                set.add(line.trim().split(Properties.Base.CTRL_A)[0]);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("AddFilterBlackListForBasedataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("AddFilterBlackListForBasedataMapper", "validOutputCt");


            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(Properties.Base.CTRL_A, -1);

            validInputCt.increment(1);

            String imei = elementsInfo[59];
            String idfa = elementsInfo[60];
            String mac = elementsInfo[61];
            String androidId = elementsInfo[62];

            if (this.deviceSet.contains(imei) ||
                    this.deviceSet.contains(idfa) ||
                    this.deviceSet.contains(mac) ||
                    this.deviceSet.contains(androidId)){
                String timestamp = elementsInfo[30];
                String ip = elementsInfo[40];
                String ipType = "";
                String ipGeo = "";
                String userAgent = elementsInfo[35];
                String geoId = elementsInfo[39];
                String postalCode = "";
                String userCluster = "";
                String interest = elementsInfo[44];
                String interestCate = "";
                String gender = elementsInfo[43];
                String os = elementsInfo[37];
                String phoneBrand = elementsInfo[49];
                String phoneType = elementsInfo[50];
                String osVersion = elementsInfo[48];
                String carrier = elementsInfo[53];
                String deviceType = elementsInfo[46];
                String screenHeight = elementsInfo[52];
                String screenWeight = elementsInfo[52];
                String isApp = elementsInfo[55];
                String appCate = elementsInfo[58];

                value_result.set("mobile" +
                        Properties.Base.CTRL_A + timestamp +
                        Properties.Base.CTRL_A + ip +
                        Properties.Base.CTRL_A + ipType +
                        Properties.Base.CTRL_A + ipGeo +
                        Properties.Base.CTRL_A + userAgent +
                        Properties.Base.CTRL_A + geoId +
                        Properties.Base.CTRL_A + postalCode +
                        Properties.Base.CTRL_A + userCluster +
                        Properties.Base.CTRL_A + interest +
                        Properties.Base.CTRL_A + interestCate +
                        Properties.Base.CTRL_A + gender +
                        Properties.Base.CTRL_A + os +
                        Properties.Base.CTRL_A + phoneBrand +
                        Properties.Base.CTRL_A + phoneType +
                        Properties.Base.CTRL_A + osVersion +
                        Properties.Base.CTRL_A + carrier +
                        Properties.Base.CTRL_A + deviceType +
                        Properties.Base.CTRL_A + screenHeight +
                        Properties.Base.CTRL_A + screenWeight +
                        Properties.Base.CTRL_A + isApp +
                        Properties.Base.CTRL_A + appCate);
                context.write(NullWritable.get(),value_result);
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

        Job job = new Job(conf,"CollectCpDataWithSpecificFeats");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectCpDataWithSpecificFeats.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String basedataPcPath = otherArgs[1];
        String basedataMobilePath = otherArgs[2];
        String trueData = otherArgs[3];
        String falseData = otherArgs[4];
        String currentData = otherArgs[5];
        job.getConfiguration().set("trueData", trueData);
        job.getConfiguration().set("falseData", falseData);

        for (int i=0;i<25;i++) {
            CommUtil.addInputFileComm(job, fs, basedataPcPath + "/log_date=" + currentData, TextInputFormat.class, pcDataMapper.class);
            CommUtil.addInputFileComm(job, fs, basedataMobilePath+"/log_date="+currentData, TextInputFormat.class, mobileDataMapper.class);
            currentData = DateUtil.getSpecifiedDayBefore(currentData);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
