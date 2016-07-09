package com.mr.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.mr.config.Properties;

public class StringUtil {
	
	public static String  ctrlOne = new String(new byte[]{0x01});
	public static String  ctrlTwo = new String(new byte[]{0x02});
	public static String  ctrlNine = new String(new byte[]{0x09});
	
	public static String  getTab()
	{
		String delit = null;
		byte[] delitByte = { 0x09 };
		delit = new String(delitByte);
		return delit;
	}
	
	public static String  getEnter()
	{
		String delit = null;
		byte[] delitByte = { 0x10,0x13 };
		delit = new String(delitByte);
		return delit;
	}
	
	public static String  getControlStr(byte [] byteStr)
	{
		String delit = null;
		delit = new String(byteStr);
		return delit;
	}
	
	public static String  getCtrOne()
	{
		String delit = null;
		byte[] delitByte = { 0x01 };
		delit = new String(delitByte);
		return delit;
	}
	
	public static String  getCtrTwo()
	{
		String delit = null;
		byte[] delitByte = { 0x02 };
		delit = new String(delitByte);
		return delit;
	}
	
	
	public static String getSplitTab(byte ctrlCode) 
	{
		String delit = null;
		
		
		return delit;
	}
	
	/**
	 * 将数组中的对象用指定分隔符连接成字符串
	 * @param sb
	 * @param split_char
	 * @param array
	 */
	public static void joinArray(StringBuffer sb, String split_char,Object[] array){
		for(int i=0;i<array.length;i++){
			sb.append(array[i]);
			if(i!=array.length-1){
				sb.append(split_char);
			}
		}
	}
	
	/**
	 * 整数数组转换成字符串
	 * @param arr
	 * @param separator
	 * @param quotation
	 * @return
	 */
	public static String convertArray2String(long[] arr, String separator, String quotation) {
        StringBuffer result = new StringBuffer();
        if (arr == null)
            return result.toString();
        for(long t : arr){
        	
            result.append(separator == null ? "" : separator);
            result.append(quotation == null ? "" : quotation);
            result.append(t);
            result.append(quotation == null ? "" : quotation);
        }
        if (separator != null)
            result.delete(0, separator.length());
        return result.toString();
    }
	
	
	public static String convertDoubleArray2String(double[] arr, String separator, String quotation) {
        StringBuffer result = new StringBuffer();
        if (arr == null)
            return result.toString();
        for(Double t : arr){
            result.append(separator == null ? "" : separator);
            result.append(quotation == null ? "" : quotation);
            result.append(t);
            result.append(quotation == null ? "" : quotation);
        }
        if (separator != null)
            result.delete(0, separator.length());
        return result.toString();
    }
	
	/* !strInfo.equals("nRLSOURCE") */
	public static Boolean isNull(String strInfo){
		strInfo = strInfo.toLowerCase();
		if ( !strInfo.equals("null")
		&& !strInfo.equals("")){
			return false;
		}
		return true;
	}
	
	public static String listToString(List list, String separator){
		String result = "";
		for(int i=0; i<list.size(); i++){
			if(i == list.size() - 1)
				result += list.get(i);
			else
				result += list.get(i) + separator;
		}
		return result;
	}

	public static String listToString(String[] list, String separator){
		String result = "";
		for(int i=0; i<list.length; i++){
			if(i == list.length - 1)
				result += list[i];
			else
				result += list[i] + separator;
		}
		return result;
	}

	public static String listToString(HashSet<String> list, String separator){
		String result = "";
		if (!list.isEmpty()) {
			for (String info : list) {
				result += info + separator;
			}
			result = result.substring(0, result.length() - separator.length());
		}
		return result;
	}
	public static String mapToString(HashMap<String,Integer> map, String separator){
		String result = "";
		if (!map.isEmpty()) {
			for (String key : map.keySet()) {
				result += key + ":" + String.valueOf(map.get(key)) + separator;
			}
			result = result.substring(0, result.length() - separator.length());
		}
		return result;
	}
	public static String mapToStringStr(HashMap<String,String> map, String separator){
		String result = "";
		if (!map.isEmpty()) {
			for (String key : map.keySet()) {
				result += key + ":" + map.get(key) + separator;
			}
			result = result.substring(0, result.length() - separator.length());
		}
		return result;
	}
}
