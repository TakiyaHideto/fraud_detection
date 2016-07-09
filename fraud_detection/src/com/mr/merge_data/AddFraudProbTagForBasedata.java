package com.mr.merge_data;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.FeaEngUtil;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by TakiyaHideto on 16/2/26.
 */
public class AddFraudProbTagForBasedata {
    public static class BasedataMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        private HashMap<String,String> featureIdMap = new HashMap<String, String>();
        private HashMap<String,String> featureWeightMap = new HashMap<String, String>();
        private FileSystem fs = null;

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String featureIdMapPath = context.getConfiguration().get("featureIdMapPath");
            String featureWeightMapPath = context.getConfiguration().get("featureWeightMapPath");
            fs = FileSystem.get(conf);
            loadCacheFeatId(fs, featureIdMapPath, this.featureIdMap);
            loadCacheFeatWeight(fs, featureWeightMapPath, this.featureWeightMap);
        }

        public static void loadCacheFeatId(FileSystem fs,String path,
                                           HashMap<String,String> map)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            while((line = br.readLine())!=null){
                String[] info = line.split(Properties.Base.BS_SEPARATOR_TAB,-1);
                String key = info[1];
                String value = info[0];
                map.put(key, value);
            }
            br.close();
        }

        public static void loadCacheFeatWeight(FileSystem fs,String path,
                                           HashMap<String,String> map)
                throws FileNotFoundException, IOException{
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line;
            int count = 1;
            while((line = br.readLine())!=null){
                String key = String.valueOf(count);
                String value = line;
                map.put(key, value);
                count ++;
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("BasedataMapper","validInputCt");
            Counter validOutputCt = context.getCounter("BasedataMapper","validOutputCt");
            Counter illegalColumnNumCt = context.getCounter("BasedataMapper","illegalColumnNumCt");

            String line = value.toString();

            validInputCt.increment(1);

//            if (line.split(Properties.Base.CTRL_A,-1).length!=45){
//                illegalColumnNumCt.increment(1);
//                return;
//            }

            String session_id = "0";
            String yoyi_cost = "1";
            String clk = "2";
            String reach = "3";
            String action = "4";
            String action_monitor_id = "5";
            String bid_way = "6";
            String algo_data = "7";
            String account_id = "8";
            String campaign_id = "9";
            String camp_cate_id = "10";
            String camp_sub_cate_id = "11";
            String order_id = "12";
            String ad_id = "13";
            String width = "14";
            String height = "15";
            String filesize = "16";
            String extname = "17";
            String adx_id = "18";
            String site_cate_id = "19";
            String content_cate_id = "20";
            String yoyi_cate_id = "21";
            String domain = "22";
            String host = "23";
            String url = "24";
            String refer_url = "25";
            String page_title = "26";
            String adzone_id = "27";
            String adzone_position = "28";
            String reserve_price = "29";
            String bid_timestamp = "30";
            String bid_date = "31";
            String weekday = "32";
            String hour = "33";
            String minute = "34";
            String user_agent = "35";
            String browser = "36";
            String os = "37";
            String language = "38";
            String area_id = "39";
            String ip = "40";
            String adx_cookie = "41";
            String yoyi_cookie = "42";
            String gender = "43";
            String profile = "44";

            line = value.toString().replaceAll(Properties.Base.BS_SEPARATOR_SPACE, "").replaceAll(":","").replaceAll("_","#");
            String[] featureValue = line.split(Properties.Base.CTRL_A,-1);

            double linearValue = 0.0;
            for (int i=0;i<45;i++){
                String featureKey = String.valueOf(i)+Properties.Base.BS_SEPARATOR_UNDERLINE+featureValue[i];
                if (this.featureIdMap.containsKey(featureKey)){
                    int featureId = Integer.parseInt(this.featureIdMap.get(featureKey));
                    linearValue += Double.parseDouble(this.featureWeightMap.get(String.valueOf(featureId+4)));
                }
            }

            double bias = Double.parseDouble(this.featureWeightMap.get("3"));
            String probability = String.valueOf(1.0-(1.0/(1+Math.exp(-linearValue-bias))));

            if(!featureValue[7].equals("")){
                featureValue[7] = probability + Properties.Base.CTRL_B + featureValue[7];
            } else {
                featureValue[7] = probability;
            }
            String newRecord = StringUtil.listToString(featureValue, Properties.Base.CTRL_A);

            value_result.set(newRecord);
            context.write(NullWritable.get(),value_result);
            validOutputCt.increment(1);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"AddFilterBlackListForBasedataMR");
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        FileSystem fs = FileSystem.get(conf);
        job.setJarByClass(AddFilterBlackListForBasedataMR.class);
//		job.setReducerClass(AddFraudProbTagForBasedataReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseDataFilePath = otherArgs[1];
        String featureIdMapPath = otherArgs[2];
        String featureWeightMapPath = otherArgs[3];
        job.getConfiguration().set("featureIdMapPath",featureIdMapPath);
        job.getConfiguration().set("featureWeightMapPath",featureWeightMapPath);

        CommUtil.addInputFileComm(job, fs, baseDataFilePath, TextInputFormat.class, BasedataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
