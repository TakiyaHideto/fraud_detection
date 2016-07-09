package com.mr.merge_data;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
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
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/2/3.
 */
public class CollectFraudSeedData {
    public static class CollectFraudSeedDataMapper extends Mapper<Object, Text, Text, NullWritable> {
        private Text key_result = new Text();
        private Text value_result = new Text();
        private HashSet<String> ipSet = new HashSet<String>();
        private HashSet<String> adzoneIdSet = new HashSet<String>();
        private HashSet<String> domainSet = new HashSet<String>();
        private HashSet<String> yoyiCookieSet = new HashSet<String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            Counter ipSetVolumeCt = context.getCounter("AddFilterBlackListForBasedataMapper", "ipSetVolumeCt");
            Configuration conf = context.getConfiguration();
            fs = FileSystem.get(conf);
            String robotFraudFilePath = (String) context.getConfiguration().get("robotFraudFilePath");
            String currentDate = context.getConfiguration().get("currentDate");
            loadRobotFraudFile(fs, robotFraudFilePath, this.ipSet, ipSetVolumeCt, currentDate);

        }

        public static void loadRobotFraudFile(FileSystem fs, String path, HashSet<String> filterSet, Counter filterCt, String currentDate)
                throws FileNotFoundException, IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while ((line = br.readLine()) != null) {
                filterCt.increment(1);
                String[] info = line.split(Properties.Base.BS_SEPARATOR_TAB);
                String ip = info[0];
                String log_date = info[1];
                if (currentDate.equals(log_date)) {
                    filterSet.add(ip+log_date);
                }
            }
            br.close();
        }

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

            if (this.ipSet.contains(ip+log_date)) {
                key_result.set(line);
                value_result.set("1");
                context.write(key_result, NullWritable.get());
                validOutputCt.increment(1);
            }

        }
    }
    public static class CollectFraudSeedDataReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // initialize Counter
            Counter validOutputCt = context.getCounter("CollectFraudSeedDataReducer", "validOutputCt");

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 7) {
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf, "CollectFraudSeedData");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(CollectFraudSeedData.class);
//		job.setReducerClass(CollectFraudSeedDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseDataFilePath = otherArgs[1];
        String robotFraudFilePath = otherArgs[2];
        String adzoneIdSetFilePath = otherArgs[3];
        String domainSetFilePath = otherArgs[4];
        String yoyiCookieSetFilePath = otherArgs[5];
        String currentDate = otherArgs[6];
        job.getConfiguration().set("currentDate",currentDate);
        job.getConfiguration().set("robotFraudFilePath", robotFraudFilePath);
//        job.getConfiguration().set("adzoneIdSetFilePath", adzoneIdSetFilePath);
//        job.getConfiguration().set("domainSetFilePath", domainSetFilePath);
//        job.getConfiguration().set("yoyiCookieSetFilePath", yoyiCookieSetFilePath);
        job.getConfiguration().set("mapred.queue.name", "algo-dev");

        CommUtil.addInputFileComm(job, fs, baseDataFilePath, TextInputFormat.class, CollectFraudSeedDataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}