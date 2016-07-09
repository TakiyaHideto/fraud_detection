package com.mr.crossplatform.backup;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
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
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/3/16.
 */
public class ExtractIpIntervalData {
    public static class ExtractIpIntervalDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> ipList = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String ipEduPath = context.getConfiguration().get("ipEduPath");
            loadCacheIp(fs, ipEduPath, this.ipList);
        }

        public static void loadCacheIp(FileSystem fs,String path,HashSet<String> ipList)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] ipInfo = line.split(Properties.Base.BS_SEPARATOR_SPACE,-1);
                String[] firstIp = ipInfo[0].split("\\.",-1);
                String[] secondIp = ipInfo[1].split("\\.",-1);
                for (int i=0;i<4;i++){
                    if (firstIp[i].equals(secondIp[i])) {
                        continue;
                    } else {
                        int lowerBound = Integer.parseInt(firstIp[i]);
                        int upperBound = Integer.parseInt(secondIp[i]);
                        if (i==0){
                            for (int j=lowerBound;j<=upperBound;j++){
                                ipList.add(String.valueOf(j));
                            }
                        } else if (i==1){
                            for (int j=lowerBound;j<=upperBound;j++){
                                ipList.add(firstIp[0] + "." + String.valueOf(j));
                            }
                        } else if (i==2){
                            for (int j=lowerBound;j<=upperBound;j++){
                                ipList.add(firstIp[0]+"."+firstIp[1]+"."+String.valueOf(j));
                            }
                        } else if (i==3){
                            for (int j=lowerBound;j<=upperBound;j++){
                                ipList.add(firstIp[0]+"."+firstIp[1]+"."+firstIp[2]+"."+String.valueOf(j));
                            }
                        }
                        break;
                    }
                }
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{

            Counter validInputCt = context.getCounter("ExtractIpIntervalData","validInputCt");
            Counter validOutputCt = context.getCounter("ExtractIpIntervalData", "validOutputCt");
            Counter tempCt = context.getCounter("ExtractIpIntervalData","tempCt");


            String line = value.toString();
            String[] elements = line.split(Properties.Base.CTRL_A,-1);

            String device = elements[0];
            String deviceType = elements[2];
            String ipInfo = elements[3];

            if (deviceType.equals("1"))
                return;

            validInputCt.increment(1);

            try {

                for (String ipTime : ipInfo.split(Properties.Base.CTRL_B, -1)) {
                    tempCt.increment(1);
                    String ip = ipTime.split(Properties.Base.CTRL_C, -1)[0];
                    String[] ipEle = ip.split("\\.", -1);
                    String ip1 = ipEle[0];
                    String ip2 = ipEle[0] + "." + ipEle[1];
                    String ip3 = ipEle[0] + "." + ipEle[1] + "." + ipEle[2];
                    String ip4 = ipEle[0] + "." + ipEle[1] + "." + ipEle[2] + "." + ipEle[3];

                    if (this.ipList.contains(ip1) || this.ipList.contains(ip2) || this.ipList.contains(ip3) || this.ipList.contains(ip4)) {
                        key_result.set(device);
                        value_result.set(deviceType);
                        context.write(key_result, value_result);
                        validOutputCt.increment(1);
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e){
                return;
            }
        }
    }

    public static class ExtractIpIntervalDataReducer extends Reducer<Text, Text, NullWritable, Text> {
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

            String deviceType = "";
            for (Text value: values){
                if (value.toString().contains("2")) {
                    deviceType = "idfa";
                    break;
                } else if (value.toString().contains("3")){
                    deviceType = "imei";
                    break;
                } else if (value.toString().contains("4")){
                    deviceType = "androidId";
                    break;
                }
            }

            mos.write(deviceType,NullWritable.get(),key,deviceType);
//            context.write(NullWritable.get(),key);
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

        Job job = new Job(conf,"ExtractIpIntervalData");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        conf.set("mapreduce.map.memory.mb ", "2000");
        conf.set("mapreduce.map.java.opts ", "-Djava.net.preferIPv4Stack=true -Xmx2000000000");

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(ExtractIpIntervalData.class);
        job.setReducerClass(ExtractIpIntervalDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);


        MultipleOutputs.addNamedOutput(job,"idfa",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"imei",TextOutputFormat.class,NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"androidId",TextOutputFormat.class,NullWritable.class, Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String bidMidLogPath = otherArgs[1];
        String ipEduPath = otherArgs[2];
        String currentData = otherArgs[3];

        job.getConfiguration().set("ipEduPath",ipEduPath);

//        for (int i=0;i<1;i++) {
//            CommUtil.addInputFileComm(job, fs,
//                    bidMidLogPath + "/" + currentData + "/IP", TextInputFormat.class, ExtractIpIntervalDataMapper.class);
//            currentData = DateUtil.getSpecifiedDayBefore(currentData);
//        }
        CommUtil.addInputFileComm(job, fs,
                bidMidLogPath, TextInputFormat.class, ExtractIpIntervalDataMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
