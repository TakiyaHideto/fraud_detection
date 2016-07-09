package com.mr.utils;

import org.omg.PortableInterceptor.INACTIVE;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	
	/*----------------------------------------------------------------
	 * 函数名:getSpecifiedDayBefore
	 * 函数功能:指定日期的前一天
	 * 参数说明:
	------------------------------------------------------------------*/
	public static String getSpecifiedDayBefore(String specifiedDay) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yy-MM-dd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int day = c.get(Calendar.DATE);
		c.set(Calendar.DATE, day - 1);

		String dayBefore = new SimpleDateFormat("yyyy-MM-dd").format(c
				.getTime());
		return dayBefore;
	}

	/*----------------------------------------------------------------
	 * 函数名:getSpecifiedDayAfter
	 * 函数功能:指定日期的后一天
	 * 参数说明:
	------------------------------------------------------------------*/
	public static String getSpecifiedDayAfter(String specifiedDay) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yy-MM-dd").parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int day = c.get(Calendar.DATE);
		c.set(Calendar.DATE, day + 1);

		String dayAfter = new SimpleDateFormat("yyyy-MM-dd").format(c
				.getTime());
		return dayAfter;
	}
	
	/*----------------------------------------------------------------
	 * 函数名:timestamp2date
	 * 函数功能:时间戳转化为Data
	 * 参数说明:时间戳信息
	------------------------------------------------------------------*/
	public static Date timestamp2date(long dlTamp) throws ParseException{
		SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
	    Long time=new Long(dlTamp);  
	    String dTime = format.format(time*1000);  
	    Date date=format.parse(dTime);  
	    
	    return date;
	}
	
	/*----------------------------------------------------------------
	 * 函数名:data2timesstamp
	 * 函数功能:日期转化为时间戳
	 * 参数说明:日期信息
	------------------------------------------------------------------*/
	public static long data2timesstamp(String strDate) throws ParseException{
		//Date或者String转化为时间戳  
	    SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
	    Date date = format.parse(strDate);  
	    return date.getTime();
	}

	/*----------------------------------------------------------------
	 * 函数名:getTimeHour
	 * 函数功能:获取时间的小时数
	 * 参数说明:时间戳信息
	------------------------------------------------------------------*/
	public static int getTimeOfHour(long dlTamp) throws ParseException{
		Date date=timestamp2date(dlTamp);
		Calendar cal=Calendar.getInstance();
		cal.setTime(date);
		
		return cal.get(Calendar.HOUR_OF_DAY);
	}

	/*----------------------------------------------------------------
	 * 函数名:getTimeHour
	 * 函数功能:获取时间的小时数
	 * 参数说明:时间戳信息
	------------------------------------------------------------------*/
	public static int getTimeMinute(long dlTamp) throws ParseException{
		Date date = timestamp2date(dlTamp);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		return cal.get(Calendar.MINUTE);
	}
	
	/*----------------------------------------------------------------
	 * 函数名:getTimeOfWeek
	 * 函数功能:获取时间星期几
	 * 参数说明:时间戳信息
	------------------------------------------------------------------*/
	public static int getTimeOfWeek(long dlTamp) throws ParseException{
		Date date=timestamp2date(dlTamp);
		Calendar cal=Calendar.getInstance();
		cal.setTime(date);
		
		return cal.get(Calendar.DAY_OF_WEEK)-1;
	}

	public static String getTimeOfDate(long dlTamp) throws ParseException{
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
		Date date = timestamp2date(dlTamp); 
		return sdf.format(date);  
	}

	public static String getActivityPeriod(String weekday,String hour){
		String activityPeriod = "";
		switch (Integer.parseInt(weekday))
		{
			case 1:
				activityPeriod = getWeekdayActivityPeriod(hour);
				break;
			case 2:
				activityPeriod = getWeekdayActivityPeriod(hour);
				break;
			case 3:
				activityPeriod = getWeekdayActivityPeriod(hour);
				break;
			case 4:
				activityPeriod = getWeekdayActivityPeriod(hour);
				break;
			case 5:
				activityPeriod = getWeekdayActivityPeriod(hour);
				break;
			case 6:
				activityPeriod = getWeekendActivityPeriod(hour);
				break;
			case 7:
				activityPeriod = getWeekendActivityPeriod(hour);
				break;
		}
		return activityPeriod;
	}

	public static String getWeekdayActivityPeriod(String hour){
		String activityPeriod = "";
		switch (Integer.parseInt(hour))
		{
			case 0:
				activityPeriod = "工作日凌晨";
				break;
			case 1:
				activityPeriod = "工作日凌晨";
				break;
			case 2:
				activityPeriod = "工作日凌晨";
				break;
			case 3:
				activityPeriod = "工作日凌晨";
				break;
			case 4:
				activityPeriod = "工作日凌晨";
				break;
			case 5:
				activityPeriod = "工作日凌晨";
				break;
			case 6:
				activityPeriod = "工作日上班出行";
				break;
			case 7:
				activityPeriod = "工作日上班出行";
				break;
			case 8:
				activityPeriod = "工作日上班出行";
				break;
			case 9:
				activityPeriod = "工作日工作上午";
				break;
			case 10:
				activityPeriod = "工作日工作上午";
				break;
			case 11:
				activityPeriod = "工作日工作上午";
				break;
			case 12:
				activityPeriod = "工作日午休";
				break;
			case 13:
				activityPeriod = "工作日午休";
				break;
			case 14:
				activityPeriod = "工作日工作下午";
				break;
			case 15:
				activityPeriod = "工作日工作下午";
				break;
			case 16:
				activityPeriod = "工作日工作下午";
				break;
			case 17:
				activityPeriod = "工作日下班回家";
				break;
			case 18:
				activityPeriod = "工作日下班回家";
				break;
			case 19:
				activityPeriod = "工作日下班回家";
				break;
			case 20:
				activityPeriod = "工作日在家休息";
				break;
			case 21:
				activityPeriod = "工作日在家休息";
				break;
			case 22:
				activityPeriod = "工作日在家休息";
				break;
			case 23:
				activityPeriod = "工作日在家休息";
				break;
		}
		return activityPeriod;
	}

	public static String getWeekendActivityPeriod(String hour){
		String activityPeriod = "";
		switch (Integer.parseInt(hour))
		{
			case 0:
				activityPeriod = "双休日凌晨";
				break;
			case 1:
				activityPeriod = "双休日凌晨";
				break;
			case 2:
				activityPeriod = "双休日凌晨";
				break;
			case 3:
				activityPeriod = "双休日凌晨";
				break;
			case 4:
				activityPeriod = "双休日凌晨";
				break;
			case 5:
				activityPeriod = "双休日凌晨";
				break;
			case 6:
				activityPeriod = "双休日凌晨";
				break;
			case 7:
				activityPeriod = "双休日上午";
				break;
			case 8:
				activityPeriod = "双休日上午";
				break;
			case 9:
				activityPeriod = "双休日上午";
				break;
			case 10:
				activityPeriod = "双休日上午";
				break;
			case 11:
				activityPeriod = "双休日上午";
				break;
			case 12:
				activityPeriod = "双休日上午";
				break;
			case 13:
				activityPeriod = "双休日下午";
				break;
			case 14:
				activityPeriod = "双休日下午";
				break;
			case 15:
				activityPeriod = "双休日下午";
				break;
			case 16:
				activityPeriod = "双休日下午";
				break;
			case 17:
				activityPeriod = "双休日下午";
				break;
			case 18:
				activityPeriod = "双休日晚上";
				break;
			case 19:
				activityPeriod = "双休日晚上";
				break;
			case 20:
				activityPeriod = "双休日晚上";
				break;
			case 21:
				activityPeriod = "双休日晚上";
				break;
			case 22:
				activityPeriod = "双休日晚上";
				break;
			case 23:
				activityPeriod = "双休日晚上";
				break;
		}
		return activityPeriod;
	}

	public static String getPeriod(String hour){
		String activityPeriod = "";
		switch (Integer.parseInt(hour))
		{
			case 0:
				activityPeriod = "1";
				break;
			case 1:
				activityPeriod = "1";
				break;
			case 2:
				activityPeriod = "1";
				break;
			case 3:
				activityPeriod = "1";
				break;
			case 4:
				activityPeriod = "1";
				break;
			case 5:
				activityPeriod = "1";
				break;
			case 6:
				activityPeriod = "2";
				break;
			case 7:
				activityPeriod = "2";
				break;
			case 8:
				activityPeriod = "2";
				break;
			case 9:
				activityPeriod = "2";
				break;
			case 10:
				activityPeriod = "2";
				break;
			case 11:
				activityPeriod = "2";
				break;
			case 12:
				activityPeriod = "3";
				break;
			case 13:
				activityPeriod = "3";
				break;
			case 14:
				activityPeriod = "4";
				break;
			case 15:
				activityPeriod = "4";
				break;
			case 16:
				activityPeriod = "4";
				break;
			case 17:
				activityPeriod = "5";
				break;
			case 18:
				activityPeriod = "5";
				break;
			case 19:
				activityPeriod = "5";
				break;
			case 20:
				activityPeriod = "6";
				break;
			case 21:
				activityPeriod = "6";
				break;
			case 22:
				activityPeriod = "6";
				break;
			case 23:
				activityPeriod = "6";
				break;
		}
		return activityPeriod;
	}

}
