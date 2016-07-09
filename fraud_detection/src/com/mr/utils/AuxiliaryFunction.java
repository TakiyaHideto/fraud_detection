package com.mr.utils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mr.config.Properties;

public class AuxiliaryFunction {

	public static Boolean adxTaobaoSession(String sessionId){
		return sessionId.substring(0,2).equals("1_");
	}

	public static Boolean adxBaiduSession(String sessionId){
		return sessionId.substring(0,2).equals("7_");
	}

	public static String extractSessionId(String idStr){
		String sessionId = "";
		String[] ids = idStr.split("&");
		for (String idString : ids){
			String[] idContent = idString.split("=");
			if (idContent[0].equals("sid")){
				sessionId = idContent[1];
				break;
			}		
		} 
		return sessionId;
	}

    public static String extractOrderId(String idStr){
        String orderId = "";
        String[] ids = idStr.split("&");
        for (String idString : ids){
            String[] idContent = idString.split("=");
            if (idContent[0].equals("oid")){
                orderId = idContent[1];
                break;
            }       
        } 
        return orderId;
    }

    public static String extractCampaignId(String idStr){
        String campaignId = "";
        String[] ids = idStr.split("&",-1);
        for (String idString : ids){
            String[] idContent = idString.split("=",-1);
            if (idContent[0].equals("cid")){
                campaignId = idContent[1];
                break;
            }       
        } 
        return campaignId;
    }

    public static String extractYoyiCookie(String idStr){
        String yoyiCookie = "";
        String[] ids = idStr.split("&");
        for (String idString : ids){
            String[] idContent = idString.split("=");
            if (idContent[0].equals("yuid")){
                yoyiCookie = idContent[1];
                break;
            }       
        } 
        return yoyiCookie;
    }

    public static String extractAdxId(String idStr){
        String yoyiCookie = "";
        String[] ids = idStr.split("&");
        for (String idString : ids){
            String[] idContent = idString.split("=");
            if (idContent[0].equals("adx_id")){
                yoyiCookie = idContent[1];
                break;
            }
        }
        return yoyiCookie;
    }

    public static Boolean chooseAdx(String sessionIdStr){
        Pattern pattern = Pattern.compile("^7_|^1_");
        Matcher matcher = pattern.matcher(sessionIdStr);
        if (matcher.matches())
            return true;
        else
            return false;
    }

    public static String listToString(List<String> featureElements){
        String temp = "";
        for(String element: featureElements){
            temp += element + Properties.Base.BS_SEPARATOR_UNDERLINE;
        }
        return temp.substring(0, temp.length());
    }

    public static String listToString(String[] featureElements, int startPos){
        String temp = "";
        for(String element: featureElements){
            temp += element + Properties.Base.BS_SEPARATOR_TAB;
        }
        return temp.substring(startPos, temp.length()-1);
    }

    public static Boolean matchBidLogType(String logData){
            Pattern pattern = Pattern.compile("bidLog");
            Matcher matcher = pattern.matcher(logData);
            if (matcher.matches())
                return true;
            else
                return false;
        }

    protected static Boolean matchWinLogType(String logData){
        Pattern pattern = Pattern.compile("winLog");
        Matcher matcher = pattern.matcher(logData);
        if (matcher.matches())
            return true;
        else
            return false;
    }

    protected static Boolean matchShowLogType(String logData){
        Pattern pattern = Pattern.compile("showLog");
        Matcher matcher = pattern.matcher(logData);
        if (matcher.matches())
            return true;
        else
            return false;
    }

    protected static Boolean matchClickLogType(String logData){
        Pattern pattern = Pattern.compile("clickLog");
        Matcher matcher = pattern.matcher(logData);
        if (matcher.matches())
            return true;
        else
            return false;
    }

    protected static Boolean checkPcBanner(String adZoneType){
        if (adZoneType.equals("true"))
            return true;
        else 
            return false;
    }
}





