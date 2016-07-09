package com.mr.feature_engineering;

import com.mr.config.Properties;
import com.mr.utils.CommUtil;
import com.mr.utils.FeaEngUtil;
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
 * Created by TakiyaHideto on 16/2/26.
 */
public class OutputNewFormRecordsForBasedata {
    public static class OutputNewFormRecordsForBasedataMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text key_result = new Text();
        private Text value_result = new Text();

        @Override
        protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
            // create counters
            Counter validInputCt = context.getCounter("CreateFeatureMapMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("CreateFeatureMapMapper", "validOutputCt");

            validInputCt.increment(1);

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

            String line = value.toString().replaceAll(Properties.Base.BS_SEPARATOR_SPACE, "").replaceAll(":","");
            String[] featureValue = line.split(Properties.Base.BS_SEPARATOR,-1);

            String label = "1";

            //选取feature
            String bidWayInfo = bid_way + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[6] + ":1";
            String campCateIdInfo = camp_cate_id + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[10] + ":1";
            String campSubCateIdInfo = camp_sub_cate_id + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[11] + ":1";
            String widthInfo = width + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[14] + ":1";
            String heightInfo = height + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[15] + ":1";
            String domainInfo = domain + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[22] + ":1";
            String hostInfo = host +Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[23] + ":1";
            String pageTitleInfo = page_title + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[26] + ":1";
            String adzoneIdInfo = adzone_id + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[27] + ":1";
            String adzonePositionInfo = adzone_position + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[28] + ":1";
            String reservePriceInfo = reserve_price + Properties.Base.BS_SEPARATOR_UNDERLINE + FeaEngUtil.transformReservePrice(featureValue[29]) + ":1";
            String weekdayInfo = weekday + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[32] + ":1";
            String hourInfo = hour + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[33] + ":1";
            String browserInfo = browser + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[36] + ":1";
            String osInfo = os + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[37] + ":1";
            String languageInfo = language + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[38] + ":1";
            String areaIdInfo = area_id + Properties.Base.BS_SEPARATOR_UNDERLINE + featureValue[39] + ":1";

            String newRecord = bidWayInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + campCateIdInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + campSubCateIdInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + widthInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + heightInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + domainInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + hostInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + pageTitleInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + adzoneIdInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + adzonePositionInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + reservePriceInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + weekdayInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + hourInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + browserInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + osInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + languageInfo +
                    Properties.Base.BS_SEPARATOR_SPACE + areaIdInfo ;

            key_result.set(String.valueOf(Math.random()));

            value_result.set(label +
                    Properties.Base.BS_SEPARATOR_SPACE + newRecord);
            context.write(NullWritable.get(), value_result);
            validOutputCt.increment(1);

        }

    }

//    public static class OutputNewFormRecordsReducer extends Reducer<Text, Text, NullWritable, Text> {
//        private Text key_result = new Text();
//        private Text value_result = new Text();
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values,Context context)
//                throws IOException, InterruptedException {
//
//            for (Text value: values){
//                context.write(NullWritable.get(), value);
//            }
//        }
//    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("<int> <out>");
            System.exit(4);
        }

        Job job = new Job(conf,"OutputNewFormRecords");
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().setBoolean("mapred.output.compress", true);
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(OutputNewFormRecords.class);
//        job.setReducerClass(OutputNewFormRecordsReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        String baseData = otherArgs[1];

        CommUtil.addInputFileComm(job, fs, baseData, TextInputFormat.class, OutputNewFormRecordsForBasedataMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
