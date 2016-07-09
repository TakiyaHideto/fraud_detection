package com.mr.code_backup.basedata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.mr.config.Properties;
import com.mr.utils.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CookieInfoMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Counter impCt;
	Counter cookieCt;
	
	protected void setup(Context context) throws IOException,InterruptedException{} 
	
	protected void cleanup(Context context) throws IOException,InterruptedException{}
	
	/*----------------------------------------------------------------
	* 函数名:valide
	* 函数功能:判断行是否为有效行(字段必须大于3个基本字段)
	* 参数说明: 有效/true 无效/false
	* 返回值类型:boolean
	------------------------------------------------------------------*/
	public boolean valide(String strLine, List<String> listLines, Counter impCt, Counter cookieCt) {
		boolean isValide = false;
		if (!StringUtil.isNull(strLine)) {
			listLines.addAll(Arrays.asList(strLine.split(Properties.Base.BS_SEPARATOR, -1)));
			if (listLines.size() >= Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT) {
				isValide = true;
				impCt.increment(1);
			} else {
				listLines.clear();
				listLines.addAll(Arrays.asList(strLine.split("\t", -1)));
				if (listLines.size() >= Properties.Base.BS_COOKIE_INPUT_COL_COUNT) {
					isValide = true;
					cookieCt.increment(1);
				}
			}
		}

		return isValide;
	}
	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String strLine = value.toString();
		List<String> listLog = new ArrayList<String>();

		impCt = context.getCounter("COOKIE_MAP_COUNT", "impCt");
		cookieCt = context.getCounter("COOKIE_MAP_COUNT", "cookieCt");
		
		if (!valide(strLine, listLog, impCt, cookieCt))  
			return;

		CookieBusinessHandle bsHandle = new CookieBusinessHandle();
		Map<String, String> mapKeyValues = new LinkedHashMap<String, String>();
		
		if (bsHandle.mapHandle(listLog, mapKeyValues) && mapKeyValues.size() > 0) {
			Set<String> keys = mapKeyValues.keySet();
			for (String k : keys){
				context.write(new Text(k), new Text(mapKeyValues.get(k)));
			}
		}
	}
}
