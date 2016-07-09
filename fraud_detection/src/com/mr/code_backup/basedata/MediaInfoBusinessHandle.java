package com.mr.code_backup.basedata;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;

import com.mr.config.Properties;
import com.mr.utils.DateUtil;
import com.mr.utils.StringUtil;
import com.mr.utils.UrlUtil;

public class MediaInfoBusinessHandle {
	public Map<String,String> mMapIgCampaign = new HashMap<String,String>();
	public Map<String,String> mMapOrderCampaign = new HashMap<String,String>();
	public Map<String,String> mMapCampaignAdcount = new HashMap<String,String>();
	public Map<String,String> mMapCampaignCategory = new HashMap<String,String>();
	public Map<String,String> mMapMediaAllCategory = new HashMap<String,String>();
	public Map<String,String> mMapMediaYoyiCategory = new HashMap<String,String>();
	public Map<String,String> mMapCookieInfo = new HashMap<String,String>();
	public Map<String,String> mMapAdvertInfo = new HashMap<String,String>();
	public Map<String,String> mMapMaterialInfo = new HashMap<String,String>();

	public MediaInfoBusinessHandle(Map<String, String> mMapCampaignCategory,
			Map<String, String> mMapIgCampaign,
			Map<String, String> mMapMediaAllCategory,
			Map<String, String> mMapMediaYoyiCategory,
			Map<String, String> mMapOrderCampaign,
			Map<String, String> mMapCampaignAdcount,
			Map<String, String> mMapCookieInfo,
			Map<String, String> mMapAdvertInfo,
			Map<String, String>	mMapMaterialInfo) {

		this.mMapIgCampaign = mMapIgCampaign;
		this.mMapOrderCampaign = mMapOrderCampaign;
		this.mMapCampaignCategory = mMapCampaignCategory;
		this.mMapMediaAllCategory = mMapMediaAllCategory;
		this.mMapMediaYoyiCategory = mMapMediaYoyiCategory;
		this.mMapCampaignAdcount = mMapCampaignAdcount;
		this.mMapCookieInfo = mMapCookieInfo;
		this.mMapAdvertInfo = mMapAdvertInfo;
		this.mMapMaterialInfo = mMapMaterialInfo;
	}
	
	private boolean checkNumber(String strSource) {
		if (strSource.length() < 1) {
			return false;
		}
		return strSource.matches("\\d+");
	}
	
	public boolean mapHandle(List<String> logList, Map<String, String> keyValues,String strCampaignID,
		boolean use_null,boolean counterCheat, Counter impCt) throws ParseException {
		if (checkNumber(logList.get(2))) {
			int iLogType = Integer.parseInt(logList.get(2));
			if (iLogType == PC_EXPOSURE_LOG_TYPE) {
				return processPcExposureLog(logList, keyValues,strCampaignID,use_null,counterCheat, impCt);
			} else if (iLogType == PC_CLICK_LOG_TYPE) {
				return processPcClickLog(logList, keyValues,strCampaignID,use_null,counterCheat);
			} else if (iLogType == PC_REACH_LOG_TYPE) {
				return processPcReachLog(logList, keyValues,strCampaignID,use_null);
			}else if (iLogType == PC_ACCESS_LOG_TYPE){
				return processAccessLog(logList,keyValues,strCampaignID,use_null);
			} else {
				return false;
			}
		}
		return false;
	}
	
	
	private String getAdInfo( List<String> listLog ,int adId_idx,int oId_idx) { 

		String strAdvertiserInfo = new String(); 

		String strAdID = listLog.get(adId_idx);// 广告ID
		String strCampaignID = ""; 				// 活动ID
		String strOrderID = listLog.get(oId_idx);	// 订单ID
		String strCampaignCategory = ""; 		// 活动分类ID
		
		if (this.mMapOrderCampaign.containsKey(strOrderID)) {
			strCampaignID = this.mMapOrderCampaign.get(strOrderID);
			
			if (mMapCampaignCategory.containsKey(strCampaignID)) {
				strCampaignCategory = mMapCampaignCategory.get(strCampaignID);
			}
		}

		strAdvertiserInfo = strAdID + Properties.Base.BS_SEPARATOR
				+ strCampaignID + Properties.Base.BS_SEPARATOR + strOrderID
				+ Properties.Base.BS_SEPARATOR + strCampaignCategory;

		return strAdvertiserInfo;
	}

	String getAdvertiserInfo(List<String> listLog ,int iOrderId){
		String strAdvertiser = "";
		String strCampaignID = "";

		String strOrderID = listLog.get(iOrderId);	// 订单ID
		if (this.mMapOrderCampaign.containsKey(strOrderID)) {
			strCampaignID = this.mMapOrderCampaign.get(strOrderID);
			if (this.mMapCampaignAdcount.containsKey(strCampaignID)) {
				strAdvertiser = this.mMapCampaignAdcount.get(strCampaignID);
			}
		}

		return strAdvertiser;
	}
	
	private String getMedia(List<String> listLog,int iUrl,int iCate,int iExchange) {
		// 媒体信息(共4个字段)
		String strMediaInfo = new String();
		String strRealURL = listLog.get(iUrl).trim();

		String strDomain = UrlUtil.getUrlDomain(strRealURL);
		String strHost = UrlUtil.getUrlHost(strRealURL);
		String strContentId = listLog.get(iCate);
		String strYoyiCategoryId = "";
		String strTmpKey = listLog.get(iExchange) + Properties.Base.BS_SEPARATOR + strContentId;
		if ( this.mMapMediaAllCategory.containsKey(strTmpKey) ) {
			strYoyiCategoryId = this.mMapMediaAllCategory.get(strTmpKey);
		}
		
		strMediaInfo = strRealURL 
				+ Properties.Base.BS_SEPARATOR + strDomain 
				+ Properties.Base.BS_SEPARATOR + strHost	
				+ Properties.Base.BS_SEPARATOR + strContentId
				+ Properties.Base.BS_SEPARATOR + strYoyiCategoryId;
		
		return strMediaInfo;

	}
	
	private String getDimensions(List<String> listLog,int iBrow,int iOs,int iArea){
		// 维度细分信息(共3个字段)
		String strDimensions = new String();
		String strBrowser = listLog.get(iBrow).trim();
		String strOS = listLog.get(iOs).trim();
		String strAreaID = listLog.get(iArea).trim();

		strDimensions = strBrowser + Properties.Base.BS_SEPARATOR + strOS
								   + Properties.Base.BS_SEPARATOR + strAreaID;
		
		return strDimensions;
		
	}
	
	private String getActionInfo(List<String> listLog,int iOne,int iTwo,int iThree){
		// 转化检测
		String strActionOneInfo = listLog.get(iOne).trim();
		String strActionTwoInfo = listLog.get(iTwo).trim();
		String strActionThreeInfo = listLog.get(iThree).trim();
		String strActionInfo = "";

		if (checkNumber(strActionOneInfo))
			strActionInfo += strActionOneInfo;
		else
			strActionInfo += "";
		
		strActionInfo += Properties.Base.BS_SEPARATOR;
		
		if (checkNumber(strActionTwoInfo))
			strActionInfo += strActionTwoInfo;
		else
			strActionInfo += "";
		
		strActionInfo += Properties.Base.BS_SEPARATOR;
		
		if (checkNumber(strActionThreeInfo))
			strActionInfo += strActionThreeInfo;
		else
			strActionInfo += "";
		
		return strActionInfo;
	}
	
	private String getClick(List<String> listLog, int iCheat, int iKeyWord, boolean counterCheat) {
		// 统计信息(共3个字段)
		String strCheat = listLog.get(iCheat).trim();
		String strNormalClickCount = "0";

		int iFlag = checkNumber(strCheat) ? Integer.parseInt(strCheat) : -1;
		if ( counterCheat ) {
			if (iFlag == 0 || iFlag == 1 || iFlag == 2) {
				if (!listLog.get(iKeyWord).trim().equals("external")) {
					strNormalClickCount = "1";
				}
			}
		}else{
			strNormalClickCount = "1";
		}

		return strNormalClickCount;
	}
	
	private String getImp(List<String> listLog, int iCheat, boolean counterCheat) {
		String strCheat = listLog.get(iCheat).trim();

		String strNormalImpressCount = "0";
		int iFlag = checkNumber(strCheat) ? Integer.parseInt(strCheat) : -1;
		if ( counterCheat ) {
			if (iFlag == 0 || iFlag == 1 || iFlag == 2) {
				strNormalImpressCount = "1";
			}
		}else{
			strNormalImpressCount = "1";
		}
		return strNormalImpressCount;
	}
	
	private String getExtra(List<String> listLog, int[] arrPos) {
		String strResult = new String();
		int iLen = arrPos.length;
		int lLen = listLog.size();

		for (int pos = 0; pos < iLen; pos++) {
			if (arrPos[pos] < lLen) {
				strResult += Properties.Base.BS_SEPARATOR
						+ listLog.get(arrPos[pos]).trim();
			}
		}

		return strResult;
	}

	private String getCampainID(List<String> listLog ,int iOrderId) {
		String strOrderID = listLog.get(iOrderId);	// 订单ID
		String strCampaignID = ""; 		// 活动分类ID
		
		if (this.mMapOrderCampaign.containsKey(strOrderID)) {
			strCampaignID = this.mMapOrderCampaign.get(strOrderID);// 活动ID
		}

		return strCampaignID;
	}
	
	private String getAdExtraInfo(String strAdId){
		String strSelectedAdInfo = new String();
		String strAdInfo = "";
		String strMaterialId = "";

		strSelectedAdInfo = "0" + Properties.Base.BS_SEPARATOR+"0"+
							 Properties.Base.BS_SEPARATOR+"0";

		if( this.mMapAdvertInfo.containsKey(strAdId) ){
			strAdInfo = this.mMapAdvertInfo.get(strAdId);
			String [] arrInfo =strAdInfo.split(Properties.Base.BS_SEPARATOR, -1);
			if (arrInfo.length != 2){
				return strSelectedAdInfo;
			}

			strMaterialId = arrInfo[1];
			if ( mMapMaterialInfo.containsKey(strMaterialId) ) {
				String strValue = mMapMaterialInfo.get(strMaterialId);
				String [] arrayValue = strValue.split(Properties.Base.BS_SEPARATOR, -1);
				if( arrayValue.length != 3 ){
					return strSelectedAdInfo;
				}

				strSelectedAdInfo = arrayValue[0] +
					Properties.Base.BS_SEPARATOR+arrayValue[1]+
					Properties.Base.BS_SEPARATOR+arrayValue[2];
			}	
		}
		
		return strSelectedAdInfo;
	}

	private String getActionInfo(List<String> listLog,int iInfo,int iOne,int iTwo,int iThree){
		// 统计信息

		String strActionOneInfo = listLog.get(iOne).trim();
		String strActionTwoInfo = listLog.get(iTwo).trim();
		String strActionThreeInfo = listLog.get(iThree).trim();
		String strActionInfo = listLog.get(iInfo).trim();
		
		if (!checkNumber(strActionInfo))
			strActionInfo = "";
		
		strActionInfo += Properties.Base.BS_SEPARATOR;

		if (checkNumber(strActionOneInfo))
			strActionInfo += strActionOneInfo;
		else
			strActionInfo += "";
		
		strActionInfo += Properties.Base.BS_SEPARATOR;
		
		if (checkNumber(strActionTwoInfo))
			strActionInfo += strActionTwoInfo;
		else
			strActionInfo += "";
		
		strActionInfo += Properties.Base.BS_SEPARATOR;
		
		if (checkNumber(strActionThreeInfo))
			strActionInfo += strActionThreeInfo;
		else
			strActionInfo += "";
		
		return strActionInfo;
	}
	
	private String getDateTime(List<String> listLog,int timestamp_idx) throws ParseException{
		String strDateTime = "" + Properties.Base.BS_SEPARATOR + "" + Properties.Base.BS_SEPARATOR + "" + Properties.Base.BS_SEPARATOR + "" + Properties.Base.BS_SEPARATOR + "";
		
		String timestamp = listLog.get(timestamp_idx).trim();
		if (checkNumber(timestamp) && timestamp.length() == 10){
			long lTimeStamp = Long.parseLong(timestamp);
			if (lTimeStamp > 0)
				strDateTime = timestamp 
						 + Properties.Base.BS_SEPARATOR + DateUtil.getTimeOfDate(lTimeStamp)
						 + Properties.Base.BS_SEPARATOR + DateUtil.getTimeOfWeek(lTimeStamp)
						 + Properties.Base.BS_SEPARATOR + DateUtil.getTimeOfHour(lTimeStamp)
						 + Properties.Base.BS_SEPARATOR + DateUtil.getTimeMinute(lTimeStamp);
		}
		
		return strDateTime;
	}
	
	/*----------------------------------------------------------------
	 * 函数名:processExposureLogs
	 * 函数功能:处理广告展现的日志
	 * 参数说明:List<String> listLog，日志内容
	 * 返回值类型:boolean
	 * 
	 * 	boolean bItye = !StringUtil.isNull(strCookieInfo) // 判断是否为空
				&& !StringUtil.isNull(strRealURL)
				&& !StringUtil.isNull(strExchangeID)
				&& StringUtil.isNull(strAdPosID);

		if (!bItye)
			return false;
	 *  *  *  *  *
	------------------------------------------------------------------*/
	private boolean processPcExposureLog(List<String> listLog,Map<String, String> mapKeyValues,
					String strCampaignID,boolean use_null,boolean counterCheat, Counter impCt) 
					throws ParseException {
		if (listLog.size() < PC_EXPOSURE_COUNT){
			return false;
		}

		String strCookieID = listLog.get(7).trim();
		if (StringUtil.isNull(strCookieID) && !use_null){
			return false;
		}

		String mCompainId  = getCampainID(listLog, 6);

		// handle single campainId
		if ( !strCampaignID.equals("All") && !mCompainId.equals(strCampaignID) ){
			return false;
		}

		String strAdInfo = getAdInfo(listLog,5,6); //
		String strSign = listLog.get(10).trim(); // 唯一标识。关联展现、点击、转化
		String strOrderID = listLog.get(6).trim();// 订单id

		String strKey = strOrderID + Properties.Base.BS_SEPARATOR + strSign;

		String strMediaInfo = getMedia(listLog,14,9,11); 		
		String strDimensions = getDimensions(listLog,28,29,16); 
		String strActionInfo = getActionInfo(listLog,32,33,34);
		String strImp = getImp(listLog, 30, counterCheat);  
		
		int [] arrPos = {4, 12, 26, 11};
		String strExtra = getExtra(listLog,arrPos);
		String strAdID = listLog.get(5).trim();

		String strAdvertiserInfo = getAdvertiserInfo(listLog,6);
		String strAdExtraInfo = getAdExtraInfo(strAdID);
		String strDateTime = getDateTime(listLog, 3);

        
		String strValue = "PC_EXPOSURE_LOG_TYPE" 
				+ Properties.Base.BS_SEPARATOR + strCookieID // cookie_id
				+ Properties.Base.BS_SEPARATOR + strMediaInfo // Url + domain + host + content id + yoyi_cate_id
				+ Properties.Base.BS_SEPARATOR + strDimensions  // browser + os + area_id		
				+ strExtra // language + ip + ad_pos_id + adx id 
				+ Properties.Base.BS_SEPARATOR + strAdInfo // ad id + campaign id + order id + campaign cate id
				+ Properties.Base.BS_SEPARATOR + strAdExtraInfo //  width + height + filesize
				+ Properties.Base.BS_SEPARATOR + strDateTime // timestamp + date + weekday + hour + minute
				+ Properties.Base.BS_SEPARATOR + strAdvertiserInfo // account id
				+ Properties.Base.BS_SEPARATOR + strActionInfo; //  three action monitors; 
		
		if (strImp.equals("1") 
				&& strKey.trim().split(Properties.Base.BS_SEPARATOR , 2).length == 2 
				&& strValue.split(Properties.Base.BS_SEPARATOR, -1).length == Properties.Base.BS_IMP_VALUE_COUNT){
			mapKeyValues.put(strKey, strValue);
			impCt.increment(1);
			return true;			
		} else {
			return false;
		}

	}

	/*----------------------------------------------------------------
	 * 函数名:processClickLogs
	 * 函数功能:处理广告点击的日志
	 * 参数说明:List<String> listLog，日志内容
	 * 返回值类型:boolean
	------------------------------------------------------------------*/
	private boolean processPcClickLog(List<String> listLog,Map<String, String> mapKeyValues,
		String strCampaignID,boolean use_null,boolean counterCheat) 
		throws ParseException {
		if (listLog.size() < PC_CLICK_COUNT)
			return false;

		String strCookieInfo = listLog.get(7).trim();
		if ( StringUtil.isNull(strCookieInfo) && !use_null){
			return false;
		}

		String mCompainId  = getCampainID(listLog,6);
		if ( !strCampaignID.equals("All") && !mCompainId.equals(strCampaignID) ){
			return false;
		}

		String strOrderID = listLog.get(6).trim(); // 订单id 
		String strSign = listLog.get(10).trim(); // 唯一标识。关联展现、点击、转化

		String strKey = strOrderID + Properties.Base.BS_SEPARATOR + strSign;
		String strClick = getClick(listLog, 32,29,counterCheat); // 统计信息
		
		String strValue = "PC_CLICK_LOG_TYPE" 
				+ Properties.Base.BS_SEPARATOR + strClick;

		if (strKey.trim().split(Properties.Base.BS_SEPARATOR , 2).length == 2){
			mapKeyValues.put(strKey, strValue);
			return true;
		} else 
			return false;
	}

	/*----------------------------------------------------------------
	 * 函数名:processPcReachLog
	 * 函数功能:处理到达日志
	 * 参数说明:List<String> listLog，日志内容
	 * 返回值类型:boolean
	------------------------------------------------------------------*/
	private boolean processPcReachLog(List<String> listLog,Map<String, String> mapKeyValues,
		String strCampaignID,boolean use_null) {
		if (listLog.size() < PC_REACH_COUNT){
			return false;
		}

		String strCookieInfo = listLog.get(5).trim();
		if (StringUtil.isNull(strCookieInfo) && !use_null){
			return false;
		}

		String mCompainId = getCampainID(listLog,11);
		if ( !strCampaignID.equals("All") && !mCompainId.equals(strCampaignID) ){
			return false;
		}

		String strOrderID = listLog.get(11).trim();
		String strSign = listLog.get(12).trim();  // 唯一标识。关联展现、点击、转化

		String strKey = strOrderID + Properties.Base.BS_SEPARATOR + strSign;

		String strValue = "PC_REACH_LOG_TYPE" 
				+ Properties.Base.BS_SEPARATOR + getActionInfo(listLog, 7, 17, 18, 19); // action info + monitor 1 + monitor 2 + monitor 3
		
		if (strKey.trim().split(Properties.Base.BS_SEPARATOR , 2).length == 2){
			mapKeyValues.put(strKey, strValue);
			return true;
		} else 
			return false;
	}
	
	private boolean processAccessLog(List<String> listLog,Map<String, String> mapKeyValues,
		String strCampaignID,boolean use_null) {
		if (listLog.size() < PC_ACCESSS_COUNT)
			return false;

		String strCookieInfo = listLog.get(14).trim();
		if (StringUtil.isNull(strCookieInfo) && !use_null )
			return false;

		String mCompainId = getCampainID(listLog,6);
		if ( !strCampaignID.equals("All") && !strCampaignID.equals(mCompainId) )
			return false;

		String strOrderID = listLog.get(6).trim(); // 订单id 
		String strSign = listLog.get(9).trim(); // 唯一标识。关联展现、点击、转化

		String strKey = strOrderID + Properties.Base.BS_SEPARATOR + strSign;
		
		String strValue = "PC_ACCESS_LOG_TYPE" 
				+ Properties.Base.BS_SEPARATOR + listLog.get(21).trim();
		
		if (strKey.trim().split(Properties.Base.BS_SEPARATOR , 2).length == 2){
			mapKeyValues.put(strKey, strValue);
			return true;
		} else 
			return false;
	}

	/* PC-广告展现日志类型 */
	private static final int PC_EXPOSURE_LOG_TYPE = 4;
	private static final int PC_EXPOSURE_COUNT = 36;
	
	/* PC-广告点击日志类型 */
	private static final int PC_CLICK_LOG_TYPE = 5;
	private static final int PC_CLICK_COUNT = 41;
	
	/* PC-广告到达日志类型 */
	private static final int PC_REACH_LOG_TYPE = 6;
	private static final int PC_REACH_COUNT = 24;
	
	/*PC-广告竞价成功日志类型  */
	private static final int PC_ACCESS_LOG_TYPE = 3;
	private static final int PC_ACCESSS_COUNT = 34;
}
