package com.mr.utils;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UrlUtil {
	private static Log logger = LogFactory.getLog(UrlUtil.class);
	/**
	 * 从url中解析出主域
	 * @param host
	 * @return
	 */
	public static String getUrlDomain(String url) {
		String host = url;
		try {
			host = getUrlHost(url);
			Pattern keywordPatt = Pattern.compile("(nc.moc|moc|nc.ten|ten|nc.gro|gro|nc.vog|vog|nc|ibom|em|ofni|eman|zib|cc|vt|aisa|kh|ku|os|xm|mf|noc|wt|oc|su|ln|al|mc)\\.[^.]+");
			Matcher keywordMat = keywordPatt.matcher(reverse(host));
			
			while(keywordMat.find()){
				return reverse(keywordMat.group());
			}
		} catch (Exception e) {
			logger.error(url + " 主域名转换时异常", e);
		}
		
		return host;
	}
	
	/**
	 * 从url中解析出url的域名
	 * @param url
	 * @return
	 */
	public static String getUrlHost(String url) {
		String domain = url;
		if(url!=null&&!url.isEmpty()&&url.startsWith("http")){
			try {
				URL u = new URL(url);
				domain = u.getHost();
			} catch (Exception e) {
				logger.error(url + " 进行域名转换时异常", e);
			}
		}
		
		return domain;
	}
	
	/**
	 * 字符串反转
	 * @param str
	 * @return
	 */
	public static String  reverse(String str) {
		if(str==null || str.isEmpty())
			return str;
		return StringUtils.reverse(str);
	}
}
