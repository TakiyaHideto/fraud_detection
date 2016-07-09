package com.mr.code_backup.basedata;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import com.mr.config.Properties;

public class CookieBusinessHandle {

	public boolean mapHandle(List<String> logList, Map<String, String> keyValues) {
		if (logList.size() < 1)
			return false;
		
		boolean isSccess = false;
		if (logList.size() >= Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT) {
			isSccess = processMediaLog(logList, keyValues);
		} else {
			isSccess = processCookieLog(logList, keyValues);
		}
		
		return isSccess && keyValues.size() > 0 ? true:false;
	}

	/*----------------------------------------------------------------
	 * 函数名:processMediaLog
	 * 函数功能:处理媒体信息
	 * 参数说明:List<String> listLog，日志内容
	 * 返回值类型:boolean
	------------------------------------------------------------------*/
	private boolean processMediaLog(List<String> listLog, Map<String, String> mapKeyValues) {
		String strCookieId = listLog.get(listLog.size() - 1).trim();
		String strValue = "PC_IMP_LOG_TYPE";
		boolean isSccess = false;
		for (int pos = 0; pos < Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT; pos++){
			strValue += Properties.Base.BS_SEPARATOR + listLog.get(pos).trim();	
		}
		
		if (strValue.split(Properties.Base.BS_SEPARATOR, -1).length == (Properties.Base.BS_FIRST_PHASE_OUT_COL_COUNT + 1)){
			isSccess = true;
		    mapKeyValues.put(strCookieId, strValue);
		}
		return isSccess;
	}

	/*----------------------------------------------------------------
	 * 函数名:processCookieLog
	 * 函数功能:处理Cookie信息
	 * 参数说明:List<String> listLog，日志内容
	 * 返回值类型:boolean
	------------------------------------------------------------------*/
	private boolean processCookieLog(List<String> listLog,Map<String, String> mapKeyValues) {
		boolean isSccess = false;
		String strCookieId = listLog.get(0).trim();
		if (strCookieId.length() > 0) {
			if (listLog.size() == Properties.Base.BS_COOKIE_INPUT_COL_COUNT) {
				String strValue = "COOKIE_LOG_TYPE"
						+ Properties.Base.BS_SEPARATOR
						+ listLog.get(1).trim()
						+ Properties.Base.BS_SEPARATOR
						+ listLog.get(2).trim()
						+ Properties.Base.BS_SEPARATOR
						+ listLog.get(3).trim();
				mapKeyValues.put(strCookieId, strValue);
				isSccess = true;
		   }
		}
		return isSccess;
	}
}
