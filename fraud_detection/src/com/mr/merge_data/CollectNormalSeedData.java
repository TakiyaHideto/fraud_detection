package com.mr.merge_data;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by TakiyaHideto on 16/2/14.
 */
public class CollectNormalSeedData {
    public static class CollectNormalSeedDataMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // create counters

            Counter validInputCt = context.getCounter("CollectFraudSeedDataMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("CollectFraudSeedDataMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] info = line.split(Properties.Base.BS_SEPARATOR, -1);
            String ip = info[40];
            String log_date = info[31];

            key_result.set(ip);
            value_result.set(line);
            context.write(key_result, value_result);
            validOutputCt.increment(1);
        }
    }

    public static class ExtractWasuInfoMapper extends Mapper<Object, Text, Text, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validOutputCt = context.getCounter("ExtractWasuInfoMapper", "validOutputCt");

            // initialization
            String line = value.toString();
            String[] elementsInfo = line.split(",", -1);

            // 日志按逗号（,）分隔，依次是
            // 0. 请求时间（2015-10-15 19:20:13）
            // 1. 完整广告请求串（GET ..）
            // 2. 状态码（??）
            // 3. 广告投放信息（user_code=m367013db4ba3d4c0001;ad_code=i5d5484f82e96d810001;stuff_code=w61cd34d6a87b5cf0001;lang=0;）
            // 4. IP（121.41.88.219）
            // 5. 服务器地址（ads.wasu.tv:8080）
            // 6. 未知参数1（"-"）
            // 7. 运行环境（"Java/1.6.0_45"）
            // 8. device ID（"SONY-DTV14-B000030F0283"）

            // 完整广告请求串中，包含了如下信息
            // 0. host ：referer
            // 1. userid：设备ID
            // 2. columnid：栏目ID
            // 3. programid：节目ID
            // 4. featureid：节目付费类别
            // 5. rate：节目码率
            // 6. ap：广告位ID

            // 其中开机ID为
            // s5d3173b25f326210001
            // g5d316f5e567cb010001
            // p5b719fa47714ad60001
            // p4c1f087434583b30001
            // a446182f61e5214a0001
            // y4045f25de7f474a0001
            // g3720de9478b27670001
            // l3720d6da0612e520001

            String dateTime = elementsInfo[0];
            String ip = elementsInfo[4];
            String date = dateTime.substring(0,"****-**-**".length());

            if (ip.split("\\.",-1).length==4) {
                key_result.set(ip);
                value_result.set("WASU_OTT_IP");

                context.write(key_result, value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static class CollectNormalSeedDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CollectFraudSeedDataReducer", "validOutputCt");

            ArrayList<String> dataSet = new ArrayList<String>();
            boolean isWasuIp = false;

            for (Text value: values){
                String valueString = value.toString();
                if (valueString.equals("WASU_OTT_IP")){
                    isWasuIp = true;
                } else {
                    dataSet.add(valueString);
                }
            }
            if (isWasuIp) {
                for (String data: dataSet){
                    value_result.set(data);
                    context.write(NullWritable.get(), value_result);
                    validOutputCt.increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf, "CollectNormalSeedData");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectNormalSeedData.class);
		job.setReducerClass(CollectNormalSeedDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseDataFilePath = otherArgs[1];
        String logWasu1 = otherArgs[2];
        String logWasu2 = otherArgs[3];
        String logWasu3 = otherArgs[4];


        CommUtil.addInputFileComm(job, fs, baseDataFilePath, TextInputFormat.class, CollectNormalSeedDataMapper.class);
        CommUtil.addInputFileComm(job, fs, logWasu1, TextInputFormat.class, ExtractWasuInfoMapper.class);
        CommUtil.addInputFileComm(job, fs, logWasu2, TextInputFormat.class, ExtractWasuInfoMapper.class);
        CommUtil.addInputFileComm(job, fs, logWasu3, TextInputFormat.class, ExtractWasuInfoMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
