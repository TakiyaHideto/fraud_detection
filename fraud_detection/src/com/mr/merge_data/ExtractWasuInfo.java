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

import java.io.IOException;

/**
 * Created by TakiyaHideto on 16/3/7.
 */
public class ExtractWasuInfo {
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
                key_result.set(ip +
                        Properties.Base.BS_SEPARATOR_TAB + date);
                value_result.set("1");

                context.write(key_result, value_result);
                validOutputCt.increment(1);
            }
        }
    }

    public static class ExtractWasuInfoReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            context.getCounter("ExtractWasuInfoReducer","validOutputCt").increment(1);
            context.write(NullWritable.get(), key);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5){
            System.err.println(otherArgs.length);
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"ExtractWasuInfo");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(ExtractWasuInfo.class);
        job.setReducerClass(ExtractWasuInfoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String log1 = otherArgs[1];
        String log2 = otherArgs[2];
        String log3 = otherArgs[3];
        String currentDate = otherArgs[4];
        job.getConfiguration().set("currentDate",currentDate);
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        String date = currentDate;

        CommUtil.addInputFileComm(job, fs, log1 + "/log_date=" + date,
                TextInputFormat.class,
                ExtractWasuInfoMapper.class);
        CommUtil.addInputFileComm(job, fs, log2+"/log_date="+date,
                TextInputFormat.class,
                ExtractWasuInfoMapper.class);
        CommUtil.addInputFileComm(job, fs, log3+"/log_date="+date,
                TextInputFormat.class,
                ExtractWasuInfoMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
