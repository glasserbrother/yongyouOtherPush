package com.zgcfo.ezg.util;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MyFormat {

	
	/**
	 * 判断字符串是否为空或长度为0
	 * 
	 * @param str字符串
	 * @return 是否为空或长度为0
	 */
	public static boolean isStrNull(String str) {
		return str == null || str.trim().length() == 0;
	}

	/**
	 * 判断字符串是否为空或长度为0
	 * 
	 * @param str 字符串
	 * @return 是否为空或长度为0
	 */
	public static boolean isStrNull(Object str) {
		return str == null || str.toString().trim().length() == 0;
	}
	
	/**
	 * 输入文本s，如果不为null，则输出s；如果为null，则输出sub
	 * @param s 要格式化的字符串
	 * @param sub 替代的字符串
	 * @return 如果不为null，则输出s；如果为null，则输出sub
	 */
	public static String formatStr(Object s, String sub) {
		return isStrNull(s) ? sub : s.toString().trim();
	}

	/**
	 * 输入文本s，如果不为null，则输出s；如果为null，则输出""
	 * @param s 要格式化的字符串
	 * @return 如果不为null，则输出s；如果为null，则输出""
	 */
	public static String formatStr(Object s) {
		return formatStr(s, "");
	}
	
	/**
	 * 获取今天(日期)
	 * @author dyh 2009-10-24
	 * @return yyyy-MM-dd
	 */
	public final static String getToday() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	
	
	public static double formatDouble(Double i) {
		return formatDouble(i, 0.0d);
	}
	
	public static double formatDouble(Object o, double dNull) {
		double i = dNull;
		if (o != null) {
			try {
				i = Double.parseDouble(o.toString());
			} catch (Exception ex) {

			}
		}
		return i;
	}
	
	public final static String formatDouble(double d, int bit) {
		String patern = "";
		if (bit <= 0) {
			bit = 0;
			patern = "0";
		} else {
			for (int i = 0; i < bit; i++) {
				patern += "#";
			}
			patern = "0." + patern;
		}
		DecimalFormat f = new DecimalFormat(patern);
		return f.format(roundingDouble(d, bit));
	}
	
	public final static double roundingDouble(double d, int digit) {
		int bit = 1;
		for (int i = 0; i < digit; i++) {
			bit *= 10;
		}
		boolean minus = d < 0;
		if (minus) {
			d = -d;
		}
		return (((long) (d * bit + 1.5)) - 1) * 1.0 / bit * (minus ? -1 : 1);
	}

	/**
	 * 输入Double对象，输出double值,如果为null，输出dNull
	 * @author dyh 2010-12-28
	 * @param i Double对象
	 * @param dNull 缺省值
	 * @return 输出数值,如果为null，输出dNull
	 */
	public static double formatDouble(Double i, int dNull) {
		return i == null ? dNull : i.doubleValue();
	}
	
	/**
	 * 获取上个月
	 * @return
	 */
	public static String getPrevMonth(){
		return getPrevMonth(getToday());
	}
	
	/**
	 * 判断15号之后
	 * @param date
	 * @return
	 */
	public static boolean isHalfPassMonth(String date){
		int day = getDay(date);
		return day > 14 ;
	}
	
	/**
	 * 判断15号之后
	 * @return
	 */
	public static boolean isHalfPassMonth(){
		int day = getDay();
		return day > 14 ;
	}
	
	
	
	/**
	 * 上上个月
	 * @return
	 */
	public static String getPrevPrevMonth(){
		return getPrevMonth(getToday(), -2);
	}
	
	public static String getPrevMonth(String date, int day){
		if (isStrNull(date))
			return "";
		String lastDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.MONTH, day);
			lastDay = df.format(calendar.getTime());
			lastDay = lastDay.substring(0,8);
			lastDay = lastDay.replaceAll("-", "");
		} catch (Exception e) {
		}
		return lastDay;
	}
	
	/**
	 * 获取日期的上个月
	 * @param date
	 * @return
	 */
	public static String getPrevMonth(String date){		
		return getPrevMonth(date, -1);
	}
	
	public static int formatInt(Object o) {
		return formatInt(o, -1);
	}

	/**
	 * 输入Object对象，输出数值,如果为null，输出intNull
	 * @author dyh 2010-12-28
	 * @param o Object对象
	 * @param intNull 缺省值
	 * @return 输出数值,如果为null，输出intNull
	 */
	public static int formatInt(Object o, int intNull) {
		int i = intNull;
		if (o != null) {
			try {
				i = Integer.parseInt(o.toString());
			} catch (Exception ex) {

			}
		}
		return i;
	}
	

	/******************* 1. 日期方法开始 ***************************/

	/**
	 * 获取Calendar
	 * @author dyh 2011-11-12
	 * @param date YYYY-MM-dd
	 * @return 如果有异常，则返回今天
	 */
	public static Calendar getCalendar(String date) {
		return getCalendar(date, "yyyy-MM-dd");
	}

	/**
	 * 获取Calendar
	 * @author dyh 2011-11-12
	 * @param date
	 * @param format date的格式
	 * @return
	 */
	public static Calendar getCalendar(String date, String format) {
		return getCalendar(date, new SimpleDateFormat(format));
	}

	/**
	 * 获取Calendar
	 * @author dyh 2011-11-12
	 * @param date
	 * @param format date的格式
	 * @return
	 */
	public static Calendar getCalendar(String date, SimpleDateFormat format) {
		Calendar calendar = Calendar.getInstance();
		if (format != null) {
			try {
				calendar.setTime(format.parse(date));
			} catch (Exception e) {
			}
		}
		return calendar;
	}

	/******************* 1.1 日期方法开始：时分秒 ***************************/
	/**
	 * 获取当前时间HH:mm:ss
	 * @author dyh 2009-10-24
	 * @return HH:mm:ss
	 */
	public final static String getCurrentTime() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		return df.format(date);
	}

	// 考核报表的时长计算
	// 输入分钟数，返回“X天X小时X分钟”
	public static String getTime(int minute) {
		String strTime = "";
		int iDay = 0, iHour = 0, iMinute = 0, tmp = 0;
		tmp = minute;
		if (tmp > 0) {
			if (tmp / 1440 >= 1) {
				iDay = tmp / 1440;
				tmp = tmp - iDay * 1440;
			}

			if (tmp / 60 >= 1) {
				iHour = tmp / 60;
				tmp = tmp - iHour * 60;
			}

			iMinute = tmp;

			if (iDay > 0)
				strTime += iDay + "天";
			if (iHour > 0)
				strTime += iHour + "小时";
			if (iMinute > 0) {
				if (iDay > 0 && iHour == 0)
					strTime += "0小时";
				strTime += iMinute + "分钟";
			}
		} else if (tmp == 0)
			strTime = "0分钟";
		return strTime;
	}

	// 输入毫秒，返回“X天X小时X分钟X秒”
	public static String formatTime(long millisecond) {
		String strTime = "";
		long iDay = 0, iHour = 0, iMinute = 0, iSecond = 0, iMillisecond = 0, tmp = 0;
		tmp = millisecond;
		if (tmp > 0) {
			if (tmp / 86400000 >= 1) {
				iDay = tmp / 86400000;
				tmp = tmp - iDay * 86400000;
			}
			if (tmp / 3600000 >= 1) {
				iHour = tmp / 3600000;
				tmp = tmp - iHour * 3600000;
			}
			if (tmp / 60000 >= 1) {
				iMinute = tmp / 60000;
				tmp = tmp - iMinute * 60000;
			}
			if (tmp / 1000 >= 1) {
				iSecond = tmp / 1000;
				tmp = tmp - iSecond * 1000;
			}
			iMillisecond = tmp;

			if (iDay > 0)
				strTime += iDay + "天";
			if (iHour > 0)
				strTime += iHour + "小时";
			if (iMinute > 0) {
				if (iDay > 0 && iHour == 0)
					strTime += "0小时";
				strTime += iMinute + "分钟";
			}
			if (iSecond > 0) {
				if (iDay > 0) {
					if (iHour == 0)
						strTime += "0小时";
					if (iMinute == 0)
						strTime += "0分钟";
				} else if (iHour > 0) {
					if (iMinute == 0)
						strTime += "0分钟";
				}
				strTime += iSecond + "秒";
			}
			if (iMillisecond > 0) {
				if (iDay > 0) {
					if (iHour == 0)
						strTime += "0小时";
					if (iMinute == 0)
						strTime += "0分钟";
					if (iSecond == 0)
						strTime += "0秒";
				} else if (iHour > 0) {
					if (iMinute == 0)
						strTime += "0分钟";
					if (iSecond == 0)
						strTime += "0秒";
				} else if (iMinute > 0) {
					;
					if (iSecond == 0)
						strTime += "0秒";
				}
				strTime += iMillisecond + "毫秒";
			}
		} else if (tmp == 0)
			strTime = "0毫秒";
		return strTime;
	}

	/**
	 * 输入当前时间，判断是否在开始和结束时间段内
	 * @author dyh 2011-1-11
	 * @param now HH:mm
	 * @param starttime HH:mm
	 * @param endtime HH:mm
	 * @return
	 */
	public static boolean isInTime(String now, String starttime, String endtime) {
		if (isStrNull(now) || isStrNull(starttime) || isStrNull(endtime)) {
			return false;
		}
		if (starttime.compareTo(endtime) > 0) {// 如果开始时间迟于结束时间，则需要对调
			String tmp = starttime;
			starttime = endtime;
			endtime = tmp;
		}
		return now.compareTo(starttime) >= 0 && now.compareTo(endtime) <= 0;
	}

	/**
	 * 判断当前时间是否在开始和结束时间段内
	 * @author dyh 2011-1-11
	 * @param starttime HH:mm
	 * @param endtime HH:mm
	 * @return
	 */
	public static boolean isInTime(String starttime, String endtime) {
		DateFormat df = new java.text.SimpleDateFormat("HH:mm");
		return isInTime(df.format(Calendar.getInstance().getTime()), starttime, endtime);
	}

	/******************* 1.1 日期方法结束：时分秒 ***************************/

	/******************* 1.2 日期方法开始：日 ***************************/
	/**
	 * 按"yyyy-MM-dd HH:mm"格式化日期
	 * @author dyh 2009-10-24
	 * @param date 日期
	 * @return 返回格式化后的日期
	 */
	public final static String getDateFormat(Date date) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		return df.format(date);
	}

	/**
	 * 按"yyyy-MM-dd HH:mm:ss"格式化日期
	 * @author dyh 2009-10-24
	 * @param date 日期
	 * @return 返回格式化后的日期
	 */
	public final static String getFullDate(Date date) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String strDate = null;
		try {
			strDate = df.format(date);
		} catch (Exception e) {
		}
		return strDate;
	}

	/**
	 * 将"yyyy-MM-dd HH:mm:ss"转化成Date
	 * @author dyh 2009-10-24
	 * @param strDate yyyy-MM-dd HH:mm:ss
	 * @return 返回Date
	 */
	public final static Date setFullDate(String strDate) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = df.parse(strDate);
		} catch (Exception e) {
			date = null;
		}
		return date;
	}


	/**
	 * 获取今天(中文日期)
	 * @author dyh 2009-10-24
	 * @return yyyy年MM月dd日
	 */
	public final static String getTodayInCN() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy年M月d日");
		return df.format(date);
	}

	/**
	 * 获取今天(yyyy-MM-dd HH:mm:ss)
	 * @author dyh 2009-10-24
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public final static String getTodayFull() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(date);
	}

	/**
	 * 获取本日
	 * @author dyh 2009-12-24
	 * @return
	 */
	public final static int getDay() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("dd");
		String day = df.format(date);
		return Integer.parseInt(day);
	}

	/**
	 * 输入Date，输出日期
	 * @author dyh 2011-01-18
	 * @param date YYYY-MM-DD
	 * @return
	 */
	public final static int getDay(String date) {
		return MyFormat.formatInt(date.substring(8));
	}

	/**
	 * 获取上月1日
	 * @author dyh 2009-10-24
	 * @return
	 */
	public final static String getFirstDayInLastMonth() {
		return getFirstDayInLastMonth(getToday());
	}

	/**
	 * 获取上月最后一日
	 * @author dyh 2010-08-26
	 * @return
	 */
	public final static String getLastDayInLastMonth() {
		return getLastDayInLastMonth(getToday());
	}

	/**
	 * 获取本月1日
	 * @author dyh 2009-10-24
	 * @return
	 */
	public final static String getFirstDayInThisMonth() {
		return getFirstDayInMonth(getToday());
	}

	/**
	 * 获取本月最后一日
	 * @author dyh 2010-08-26
	 * @return
	 */
	public final static String getLastDayInThisMonth() {
		return getLastDayInMonth(getToday());
	}

	/**
	 * 获取下月1日
	 * @author dyh 2010-08-26
	 * @return
	 */
	public final static String getFirstDayInNextMonth() {
		return getFirstDayInNextMonth(getToday());
	}

	/**
	 * 获取下月最后一日
	 * @author dyh 2010-08-26
	 * @return
	 */
	public final static String getLastDayInNextMonth() {
		return getLastDayInNextMonth(getToday());
	}

	/**
	 * 获取输入日期的上月1日
	 * @author dyh 2010-03-21
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getFirstDayInLastMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		String lastDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.MONTH, -1);
			lastDay = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return lastDay.substring(0, 7) + "-01";
	}


	/**
	 * 获取输入日期的上月最后一日
	 * @author dyh 2010-08-21
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getLastDayInLastMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date.substring(0, 7) + "-01"));
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			date = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return date;
	}
	

	/**
	 * 获取输入日期的本月1日
	 * @author dyh 2010-03-21
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getFirstDayInMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		return date.substring(0, 7) + "-01";
	}

	/**
	 * 获取输入日期的本月最后一日
	 * @author dyh 2010-08-17
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getLastDayInMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date.substring(0, 7) + "-01"));
			calendar.add(Calendar.MONTH, 1);
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			date = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return date;
	}

	/**
	 * 获取输入日期的下月1日
	 * @author dyh 2010-08-26
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getFirstDayInNextMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		String lastDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.MONTH, 1);
			lastDay = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return lastDay.substring(0, 7) + "-01";
	}

	/**
	 * 获取输入日期的下月最后一日
	 * @author dyh 2010-08-21
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getLastDayInNextMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date.substring(0, 7) + "-01"));
			calendar.add(Calendar.MONTH, 2);
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			date = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return date;
	}

	/**
	 * 获取今年第1天
	 * @author dyh 2010-12-31
	 * @return
	 */
	public final static String getFirstDayInThisYear() {
		return getFirstDayInThisYear(getToday());
	}

	/**
	 * 获取输入日期的当年第1天
	 * @author dyh 2010-12-31
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getFirstDayInThisYear(String date) {
		if (date == null || date.length() != 10)
			return "";
		return date.substring(0, 4) + "-01-01";
	}

	/**
	 * 输入X年第Y月，输出起始日期
	 * @author dyh 2010-03-31
	 * @param year 年份
	 * @prama monthInYear 第Y月
	 * @param dates 输出起始日期
	 * @return
	 */
	public final static void getDayByMonthInYear(int year, int monthInYear, String dates[]) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, monthInYear - 1);
		c.set(Calendar.DAY_OF_MONTH, 1);
		dates[0] = df.format(c.getTime());

		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, monthInYear);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.add(Calendar.DAY_OF_MONTH, -1);
		dates[1] = df.format(c.getTime());
	}

	/**
	 * 获取输入日期的上月同一日
	 * @author dyh 2010-03-21
	 * @param date yyyy-mm-dd
	 * @return
	 */
	public final static String getSameDayInLastMonth(String date) {
		if (date == null || date.length() != 10)
			return "";
		String lastDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.MONTH, -1);
			lastDay = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return lastDay;
	}

	/**
	 * 按DateFormat格式返回今天
	 * @author dyh 2009-10-24
	 * @param DateFormat SimpleDateFormat格式
	 * @return 按DateFormat格式返回今天
	 */
	public final static String getToday(String DateFormat) {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat(DateFormat);
		return df.format(date);
	}

	/**
	 * 输入Date，输出YYYY-MM-dd
	 * @author dyh 2009-10-24
	 * @param date YYYY-MM-DD
	 * @return YYYY-MM-dd
	 */
	public final static String getDate(Date date) {
		if (date == null) {
			return "";
		}
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}

	/**
	 * 输入Date，按DateFormat格式输出日期
	 * @author dyh 2009-10-24
	 * @param date
	 * @param DateFormat SimpleDateFormat格式
	 * @return SimpleDateFormat格式的日期
	 */
	public final static String getDate(Date date, String DateFormat) {
		if (date == null) {
			return "";
		}
		SimpleDateFormat df = new SimpleDateFormat(DateFormat);
		return df.format(date);
	}

	/**
	 * 获取上个月的今天
	 * @author dyh 2009-10-24
	 * @param date
	 * @return YYYY-MM-dd
	 */
	public final static String getDayInLastMonth() {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.MONTH, -1);
		return getDate(c.getTime());
	}

	/**
	 * 获取前七日的今天
	 * @author dyh 2009-10-24
	 * @param date
	 * @return YYYY-MM-dd
	 */
	public final static String getDayInLastWeek() {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.WEEK_OF_YEAR, -1);
		return getDate(c.getTime());
	}


	/**
	 * 获取今后七日
	 * @author dyh 2009-11-24
	 * @param date
	 * @return YYYY-MM-dd
	 */
	public final static String getDayInNextWeek() {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.WEEK_OF_YEAR, 1);
		return getDate(c.getTime());
	}

	/**
	 * 获取离days天的年月日
	 * @author dyh 2009-10-24
	 * @param days 负数表示历史日期，正数表示未来日期
	 * @return YYYY-MM-dd
	 */
	public final static String getDate(int days) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_YEAR, days);
		return getDate(c.getTime());
	}

	/**
	 * 获取输入日期过去days天的年月日
	 * @author dyh 2010-03-21
	 * @param days 负数表示历史日期，正数表示未来日期
	 * @return YYYY-MM-dd
	 */
	public final static String getDate(int days, String date) {
		if (date == null || date.length() != 10)
			return "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c = Calendar.getInstance();
			c.setTime(df.parse(date));
			c.add(Calendar.DAY_OF_YEAR, days);
			date = df.format(c.getTime());
		} catch (Exception e) {
		}
		return date;
	}

	/**
	 * 获取第二天(日期)
	 * @author dyh 2009-10-24
	 * @param date 当前日期(YYYY-MM-DD)
	 * @return 第二天日期(YYYY-MM-DD)
	 */
	public final static String getTomorrow(String date) {
		if (date == null || date.length() != 10)
			return "";
		String nextDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.DAY_OF_YEAR, 1);
			nextDay = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return nextDay;
	}

	/**
	 * 获取前一天(日期)
	 * @author dyh 2009-10-24
	 * @param date 当前日期(YYYY-MM-DD)
	 * @return 前一天日期(YYYY-MM-DD)
	 */
	public final static String getYesterday(String date) {
		if (date == null || date.length() != 10)
			return "";
		String lastDay = "";
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(df.parse(date));
			calendar.add(Calendar.DAY_OF_YEAR, -1);
			lastDay = df.format(calendar.getTime());
		} catch (Exception e) {
		}
		return lastDay;
	}

	/**
	 * 获取前一天日期（YYYY年MM月DD日）
	 * @author YOUCL 2011-1-14
	 * @return
	 */
	public final static String getYesterdayInCN() {
		return getYesterday(getToday());
	}

	/**
	 * 获取明天(日期)
	 * @author dyh 2009-10-24
	 * @return 明天日期(YYYY-MM-DD)
	 */
	public final static String getTomorrow() {
		return getDate(1);
	}

	/**
	 * 获取昨天(日期)
	 * @author dyh 2009-10-24
	 * @return 昨天日期(YYYY-MM-DD)
	 */
	public final static String getYesterday() {
		return getDate(-1);
	}

	/**
	 * 获取两个日期相隔天数
	 * @author dyh 2010-08-05
	 * @param startdate 开始日期YYYY-MM-DD
	 * @param enddate 结束日期YYYY-MM-DD
	 * @return 两个日期相隔天数
	 */
	public final static int getDayInterval(String startdate, String enddate) {
		int interval = 0;
		if (!isStrNull(startdate) && !isStrNull(enddate)) {
			try {
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				Date d1 = df.parse(startdate);
				Date d2 = df.parse(enddate);
				long tmp = d2.getTime() - d1.getTime();
				interval = (int) (tmp / 86400000);
			} catch (Exception e) {
			}
		}
		return interval;
	}

	/**
	 * 输入YYYY-MM-DD，输出YYYY年M月D日
	 * @author dyh 2009-10-24
	 * @param date YYYY-MM-DD
	 * @return YYYY年M月D日
	 */
	public final static String getDateInCN(String date) {
		if (date == null || date.length() != 10)
			return "";
		return date.substring(0, 4) + "年" + getRealNumber(date.substring(5, 7)) + "月" + getRealNumber(date.substring(8, 10)) + "日";
	}

	/**
	 * 输入YYYY-MM-DD，输出M月D日
	 * @author dyh 2010-03-24
	 * @param date YYYY-MM-DD
	 * @return M年D日
	 */
	public final static String getDateInCNNoYear(String date) {
		if (date == null || date.length() != 10)
			return "";
		return getRealNumber(date.substring(5, 7)) + "月" + getRealNumber(date.substring(8, 10)) + "日";
	}

	/**
	 * 输入起止日期，如果起始日相同，则输出开始日期；如果同年，则输出：YYYY年M月D日-M月D日；如果不同年，则输出YYYY年M月D日-YYYY年M月D日
	 * @author dyh 2009-10-24
	 * @param startdate YYYY-MM-DD
	 * @param enddate YYYY-MM-DD
	 * @return YYYY年M月D日
	 */
	public final static String getStartAndEndDate(String startdate, String enddate) {
		return getStartAndEndDate(startdate, enddate, true, true);
	}

	/**
	 * 输入起止日期，根据年月日相同情况，分别显示最少的信息。
	 * @author dyh 2011-09-16
	 * @param startdate YYYY-MM-DD
	 * @param enddate YYYY-MM-DD
	 * @return YYYY年M月D日
	 */
	public final static String getStartAndEndDateInMonth(String startdate, String enddate) {
		return getStartAndEndDate(startdate, enddate, false, false);
	}

	/**
	 * 输入起止日期，根据年月日相同情况，分别显示最少的信息。比如：年相同，则不显示年。但无论如何至少显示月份
	 * @author dyh 2011-09-16
	 * @param startdate YYYY-MM-DD
	 * @param enddate YYYY-MM-DD
	 * @return YYYY年M年D日
	 */
	public final static String getStartAndEndDateInShort(String startdate, String enddate) {
		return getStartAndEndDate(startdate, enddate, false, true);
	}
	
	/**
	 * 输入Date类型，起止日期，根据年月日相同情况，分别显示最少的信息。比如：年相同，则不显示年。但无论如何至少显示月份
	 * @author wzx
	 * @Date Apr 25, 2012
	 * @param startdate
	 * @param enddate
	 * @return
	 */
	public final static String getStartAndEndDateInShortForDate(Date startdate, Date enddate) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String startdateString = null;
		String enddateString = null;
		try {
			startdateString = df.format(startdate);
			enddateString = df.format(enddate);
		} catch (Exception e) {
		}
		return getStartAndEndDate(startdateString, enddateString, false, true);
	}

	/**
	 * 输入起止日期，根据年月日相同情况，合并相同的年月日，分别显示最少的信息。比如：年相同，则不显示年
	 * @author dyh 2011-09-16
	 * @param startdate YYYY-MM-DD
	 * @param enddate YYYY-MM-DD
	 * @param showYear 如果起止日期年相同，是否显示年
	 * @param showMonth 如果起止日期年相同且月相同，是否显示月
	 * @return YYYY年M年D日
	 */
	public final static String getStartAndEndDate(String startdate, String enddate, boolean showYear, boolean showMonth) {
		if (startdate == null || enddate == null || startdate.length() != 10 || enddate.length() != 10)
			return "";
		int startyear = MyFormat.formatInt(startdate.substring(0, 4), 0);
		int startmonth = MyFormat.formatInt(startdate.substring(5, 7), 0);
		int startday = MyFormat.formatInt(startdate.substring(8), 0);
		int endyear = MyFormat.formatInt(enddate.substring(0, 4), 0);
		int endmonth = MyFormat.formatInt(enddate.substring(5, 7), 0);
		int endday = MyFormat.formatInt(enddate.substring(8), 0);
		StringBuilder dateOK = new StringBuilder();
		if (startyear == endyear) {// 如果年相同
			if (showYear) {
				dateOK.append(String.valueOf(startyear));
				dateOK.append("年");
			}
			if (startmonth == endmonth) {
				if (showMonth) {
					dateOK.append(String.valueOf(startmonth));
					dateOK.append("月");
				}
				dateOK.append(String.valueOf(startday));
				if (startday == endday) {

				} else {
					dateOK.append("-");
					dateOK.append(String.valueOf(endday));
				}
				dateOK.append("日");
			} else {
				dateOK.append(String.valueOf(startmonth));
				dateOK.append("月");
				dateOK.append(String.valueOf(startday));
				dateOK.append("日-");
				dateOK.append(String.valueOf(endmonth));
				dateOK.append("月");
				dateOK.append(String.valueOf(endday));
				dateOK.append("日");
			}
		} else {
			dateOK.append(String.valueOf(startyear));
			dateOK.append("年");
			dateOK.append(String.valueOf(startmonth));
			dateOK.append("月");
			dateOK.append(String.valueOf(startday));
			dateOK.append("日-");
			dateOK.append(String.valueOf(endyear));
			dateOK.append("年");
			dateOK.append(String.valueOf(endmonth));
			dateOK.append("月");
			dateOK.append(String.valueOf(endday));
			dateOK.append("日");
		}
		return dateOK.toString();
	}

	/**
	 * 输入起止日期，如果起始日相同，则输出开始日期；如果同年，则输出：YYYY年M月D日0时-M月D日24时；如果不同年，则输出YYYY年M月D日0时-YYYY年M月D日24时
	 * @author dyh 2010-08-21
	 * @param startdate YYYY-MM-DD
	 * @param enddate YYYY-MM-DD
	 * @return YYYY年M年D日
	 */
	public final static String getValidDate(String startdate, String enddate) {
		String date = getStartAndEndDate(startdate, enddate);
		String validdate = "";
		if (!isStrNull(date)) {
			int index = date.indexOf("-");
			if (index >= 0) {
				validdate = date.substring(0, index) + "0时-" + date.substring(index + 1) + "24时";
			} else {
				validdate = date + "0时-24时";
			}
		}
		return validdate;
	}

	/**
	 * 规格化日期：输入long，输出YYYY-MM-DD hh:mm:ss
	 * @author dyh 2010-07-16
	 * @param date 时间值
	 * @return YYYY-MM-DD hh:mm:ss
	 */
	public static String long2DateStr(long d) {
		Date date = new java.util.Date(d);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(date);
	}

	// 检查时间格式是否正确
	public static boolean checkDate(String strDate, String pattern) {
		boolean ok = false;
		if (strDate != null && strDate.length() >= pattern.length()) {
			try {
				java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(pattern);
				df.setLenient(false);
				df.parse(strDate);
				ok = true;
			} catch (Exception e) {
				System.out.println("Common.java(checkDate) error");
			}
		}
		return ok;
	}

	/**
	 * 将YYYYMMDD格式化为YYYY-MM-DD，其中“-”可自定义
	 * @param strDate YYYYMMDD
	 * @param pattern 间隔符：-，或中文；否则原样返回
	 * @return
	 */
	public static String formatDate(String strDate, String pattern) {
		if ("-".equals(pattern))
			return strDate.substring(0, 4) + "-" + strDate.substring(4, 6) + "-" + strDate.substring(6, 8);
		else if ("年月日".equals(pattern)) {
			String tmp1 = strDate.substring(4, 6);
			String tmp2 = strDate.substring(6, 8);
			return strDate.substring(0, 4) + "年" + (tmp1.startsWith("0") ? tmp1.substring(1) : tmp1) + "月"
					+ (tmp2.startsWith("0") ? tmp2.substring(1) : tmp2) + "日";
		} else
			return strDate;
	}

	/**
	 * 日期比较，返回较小日期
	 * @author youcl 2011-11-23
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static String compareSmallDate(String date1, String date2) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			if (df.parse(date1).after(df.parse(date2)))
				return date2;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date1;
	}

	/******************* 1.2 日期方法结束：日 ***************************/

	/******************* 1.3 日期方法开始：周 ***************************/
	/**
	 * 获取当前日期的周数
	 * @author dyh 2010-03-31
	 * @return
	 */
	public final static int getWeekInYear() {
		Calendar c = Calendar.getInstance();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int week = c.get(Calendar.WEEK_OF_YEAR);
		if (week == 1) {
			int month = c.get(Calendar.MONTH) + 1;
			if (month == 12) {
				week = MyFormat.getTotalWeeksInYear();
			}
		}
		return week;
	}

	/**
	 * 获取输入日期的周数
	 * @author dyh 2011-11-19
	 * @param date
	 * @return
	 */
	public final static int getWeekInYear(String date) {
		Calendar c = getCalendar(date);
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int week = c.get(Calendar.WEEK_OF_YEAR);
		if (week == 1) {
			int month = c.get(Calendar.MONTH) + 1;
			if (month == 12) {
				week = MyFormat.getTotalWeeksInYear(c.get(Calendar.YEAR));
			}
		}
		return week;
	}

	/**
	 * 获取今年总周数
	 * @author dyh 2011-11-15
	 * @return
	 */
	public final static int getTotalWeeksInYear() {
		return getTotalWeeksInYear(getYear());
	}

	/**
	 * 输入年份，输出该年全年周数
	 * @author dyh 2010-03-31
	 * @param year 年份
	 * @return
	 */
	public final static int getTotalWeeksInYear(int year) {
		Calendar c = Calendar.getInstance();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.set(year + 1, 0, 1);
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c.getMaximum(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 输入X年第Y周，输出起止日期
	 * @author dyh 2010-03-31
	 * @param year 年份
	 * @prama weekInYear 第Y周
	 * @return 起止日期
	 */
	public final static String[] getWeekInYear(int year, int weekInYear) {
		String dates[] = { "", "" };
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.set(Calendar.YEAR, year);
		c.set(year, 0, 1);
		int firstday = c.get(Calendar.DAY_OF_WEEK);
		c.set(Calendar.DAY_OF_YEAR, (7 - firstday + 3) + 7 * (weekInYear - 2));
		dates[0] = df.format(c.getTime());
		c.set(Calendar.YEAR, year);
		c.set(Calendar.DAY_OF_YEAR, (7 - firstday + 2) + 7 * (weekInYear - 1));
		dates[1] = df.format(c.getTime());
		return dates;
	}

	/**
	 * 输入今天所在周的周一和周日
	 * @author 孙汉斌 2011-11-07
	 * @return 起止日期
	 */
	public static String[] getWeek() {
		return getWeek(null, 0);
	}

	/**
	 * 输入日期输出该日期所在周的周一和周日
	 * @author 孙汉斌 2011-11-07
	 * @param date
	 * @return 起止日期
	 */
	public static String[] getWeek(String date) {
		return getWeek(date, 0);
	}

	/**
	 * 输入周次,输出今天对应周次的周一与周日。
	 * @author 孙汉斌 2011-11-09
	 * @param week 距离本周的周数。0表示本周，1表示下1周，-1表示上1周，依次类推
	 * @return 起止日期
	 */
	public static String[] getWeek(int week) {
		return getWeek(null, week);
	}

	/**
	 * 输入日期,周次,输出对应日期对应周次的周一与周日。若日期为空,则默认为当天
	 * @author 孙汉斌 2011-11-09
	 * @param date 输入日期(缺省值为今天)
	 * @param week 距离输入日期所在周的周数。0表示本周，1表示下1周，-1表示上1周，依次类推
	 * @return 起止日期
	 */
	public static String[] getWeek(String date, int week) {
		String dates[] = { "", "" };
		if (MyFormat.isStrNull(date))
			date = MyFormat.getToday();
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Date formatdate = df.parse(date);
			Calendar c = Calendar.getInstance();
			c.setFirstDayOfWeek(Calendar.MONDAY);// 设置星期首日为周一
			c.setTime(formatdate);// 传入日期值
			c.add(Calendar.WEEK_OF_MONTH, week);// 传入周次
			c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			String startdate = df.format(c.getTime());
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			String enddate = df.format(c.getTime());
			dates[0] = startdate;
			dates[1] = enddate;
		} catch (Exception e) {
		}
		return dates;
	}

	/**
	 * 判断今日是否是周末
	 * @author dyh 2010-03-22
	 * @param true：周末,false：非周末
	 * @return
	 */
	public static boolean TodayIsWeekend() {
		Calendar calendar = Calendar.getInstance();
		int day = calendar.get(Calendar.DAY_OF_WEEK);
		return (day == 1 || day == 7);
	}

	/**
	 * 获取今日是周几
	 * @author dyh 2011-10-25
	 * @return
	 */
	public static int getWeekDay() {
		Calendar c = Calendar.getInstance();
		return c.get(Calendar.DAY_OF_WEEK);
	}

	/**
	 * 检测是否在设置的星期时间内，比如周一10点到周二17点
	 * @author dyh 2011-11-14
	 * @return
	 */
	public static boolean inWeekDay(int firstDay, int firstTime, int lastDay, int lastTime) {
		boolean ok = false;
		Calendar now = Calendar.getInstance();
		int weekday = now.get(Calendar.DAY_OF_WEEK);// 今日
		if (weekday >= firstDay && weekday <= lastDay) {
			int hour = now.get(Calendar.HOUR_OF_DAY);
			if ((weekday == firstDay && hour >= firstTime) || (weekday == lastDay && hour < lastTime)) {
				ok = true;
			}
		}
		return ok;
	}

	/**
	 * 获取未来N周的起止日期集合
	 * @author dyh 2011-11-14
	 * @param weekcount 未来N周
	 * @return 未来N周起止日期集合
	 */
	public static String[][] getWeeks(int weekcount) {
		if (weekcount <= 0)
			weekcount = 1;
		String dates[][] = new String[weekcount][2];
		for (int i = 1; i <= weekcount; i++) {
			dates[i - 1] = getWeek(i);
		}
		return dates;
	}

	/**
	 * 获取输入日期开始的N周的起止日期集合
	 * @author dyh 2011-11-14
	 * @param firstdate 首日
	 * @param weekcount 未来N周
	 * @return 未来N周起止日期集合
	 */
	public static String[][] getWeeks(String firstdate, int weekcount) {
		if (weekcount <= 0)
			weekcount = 1;
		String dates[][] = new String[weekcount][2];
		for (int i = 0; i < weekcount; i++) {
			dates[i] = getWeek(firstdate, i);
		}
		return dates;
	}

	/******************* 1.3 日期方法结束：周 ***************************/

	/******************* 1.4 日期方法开始：月 ***************************/
	/**
	 * 获取本月
	 * @author dyh 2009-10-24
	 * @return
	 */
	public final static int getMonth() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("MM");
		String month = df.format(date);
		return Integer.parseInt(month);
	}

	/**
	 * 获取本月(YYYY-MM)
	 * @author dyh 2011-12-06
	 * @return
	 */
	public final static String getYearMonth() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
		return df.format(date);
	}

	/**
	 * 获取距离monthcount个月的月份(YYYY-MM)
	 * @author dyh 2012-01-23
	 * @param monthcount 距离本月的月数，正数表示未来月份，负数表示过去月份
	 * @return
	 */
	public final static String getYearMonth(int monthcount) {
		int year = MyFormat.getYear();
		int month = MyFormat.getMonth();
		int newmonth = month + monthcount;
		int newyear = newmonth / 12 + year;
		if (newmonth <= 0 || (newmonth > 0 && newmonth % 12 == 0)) {
			newyear -= 1;
		}
		newmonth = newmonth % 12;
		if (newmonth <= 0) {
			newmonth += 12;
		}
		return newyear + "-" + MyFormat.getFixedNumber(newmonth, 2);
	}

	/**
	 * 输入日期，输出月份
	 * @author dyh 2011-01-18
	 * @param date 日期(YYYY-MM-DD)
	 * @return
	 */
	public final static String getMonth(String date) {
		return date.substring(0, 7);
	}

	/**
	 * 输入终端公司报表使用的月份(从2011年7月起)
	 * @author dyh 2011-12-02
	 * @return
	 */
	public final static Map<String, String> getMonthZD() {
		return getMonths("2011-07");
	}

	/**
	 * 输入从开始月到当前月的所有年月
	 * @author dyh 2011-12-3
	 * @param startmonth
	 * @return
	 */
	public final static Map<String, String> getMonths(String startmonth) {
		return MyFormat.getMonth(startmonth, MyFormat.getMonth(MyFormat.getYesterday()));
	}

	/**
	 * 输入日期，输出该日为第几月
	 * @author dyh 2011-01-18
	 * @param date 日期(YYYY-MM-DD)
	 * @return
	 */
	public final static int getMonthInt(String date) {
		return MyFormat.formatInt(date.substring(5, 7));
	}

	/**
	 * 输入年和月，输出YYYY-MM
	 * @author dyh 2011-08-04
	 * @param date YYYY-MM
	 * @return YYYY年M月
	 */
	public final static String getMonth(int year, int month) {
		return year + "-" + getFixedNumber(month, 2);
	}

	/**
	 * 输入起止月份，输出月份列表
	 * @author dyh 2011-08-04
	 * @param startmonth YYYY-MM
	 * @param endmonth YYYY-MM
	 * @return 月份列表
	 */
	public final static Map<String, String> getMonth(String startmonth, String endmonth) {
		Map<String, String> hMonth = new LinkedHashMap<String, String>();
		int year_start = getYear(startmonth);
		int month_start = getMonthInt(startmonth);
		int year_end = getYear(endmonth);
		int month_end = getMonthInt(endmonth);

		int year = 0;
		int month = 0;
		int month_max = 12;
		String monthok = "";
		for (year = year_start, month = month_start; year <= year_end; year++) {
			if (year > year_start) {
				month = 1;
			}
			if (year == year_end) {
				month_max = month_end;
			} else {
				month_max = 12;
			}
			for (; month <= month_max; month++) {
				monthok = MyFormat.getMonth(year, month);
				hMonth.put(monthok, MyFormat.getMonthInCN(monthok));
			}
		}
		return hMonth;
	}

	/**
	 * 获取输入月份或日期的上一月
	 * @author dyh 2011-09-16
	 * @param month yyyy-mm
	 * @return
	 */
	public final static String getLastMonth(String month) {
		if (month == null)
			return "";
		String date = "";
		if (month.length() == 10) {
			date = month;
		} else if (month.length() == 7) {
			date = month + "-01";
		} else {
			return "";
		}
		return MyFormat.getFirstDayInLastMonth(date).substring(0, 7);
	}

	/**
	 * 获取输入月份或日期的上一月
	 * @author dyh 2011-09-16
	 * @param month yyyy-mm
	 * @return
	 */
	public final static String getLastMonthInCN(String month) {
		return getMonthInCN(getLastMonth(month));
	}

	/**
	 * 输入YYYY-MM，输出YYYY年M月
	 * @author dyh 2011-08-04
	 * @param date YYYY-MM
	 * @return YYYY年M月
	 */
	public final static String getMonthInCN(String month) {
		if (month == null || month.length() != 7)
			return "";
		return month.substring(0, 4) + "年" + getRealNumber(month.substring(5)) + "月";
	}

	/**
	 * 输入起止月份，输出这两个月份间的全部日数(比如输入2011-08和2011-10，则输出8月、9月和10月的天数，为31+30+31=92天)
	 * @author dyh 2011-08-04
	 * @param startmonth YYYY-MM
	 * @param endmonth YYYY-MM
	 * @return
	 */
	public final static int getDayBetweenMonths(String startmonth, String endmonth) {
		return getDayInterval(startmonth + "-01", getLastDayInMonth(endmonth + "-01")) + 1;
	}

	/**
	 * 获取指定年月起止月份数组,只能统计相邻两年 如输入2010,1,2010,2返回{'2010-1','2010-2'}
	 * @author ycl 2010-05-28
	 * @param startyear
	 * @param startmonth
	 * @param endyear
	 * @param endmonth
	 * @return
	 */
	public final static String[] getStartEndMonthInYear(int startyear, int startmonth, int endyear, int endmonth) {
		String month[] = null;
		int row = getSpaceMonthInYear(startyear, startmonth, endyear, endmonth);
		month = new String[row];
		int step = 0;
		if (endyear > startyear) {
			for (int i = startmonth; i <= 12; i++) {
				month[step] = startyear + "-" + i;
				step++;
			}
			for (int i = 1; i <= endmonth; i++) {
				month[step] = endyear + "-" + i;
				step++;
			}
		} else {
			for (int i = startmonth; i <= endmonth; i++) {
				month[step] = startyear + "-" + i;
				step++;
			}
		}
		return month;
	}

	/**
	 * 获取指定年月相差的月份数,(如输入2009,1,2010,3返回15)
	 * @author ycl 2010-05-28
	 * @param startyear
	 * @param startmonth
	 * @param endyear
	 * @param endmonth
	 * @return
	 */
	public final static int getSpaceMonthInYear(int startyear, int startmonth, int endyear, int endmonth) {
		int space = 0;
		if (endyear > startyear)
			space += (endyear - startyear) * 12;
		space += (endmonth - startmonth) + 1;
		return space;
	}

	/**
	 * 获取指定年月第一日,yyyy-MM-dd(如输入2010,1,返回2010-01-01)
	 * @author ycl 2010-05-28
	 * @param year 年份
	 * @prama month 第Y月
	 * @return
	 */
	public final static String getFirstDayInMonth(int year, int month) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month - 1);
		c.set(Calendar.DAY_OF_MONTH, 1);
		return df.format(c.getTime());
	}

	/**
	 * 获取指定年月最后一日,yyyy-MM-dd(如输入2010,1,返回2010-01-31)
	 * @author ycl 2010-05-28
	 * @param year 年份
	 * @prama month 第Y月
	 * @return
	 */
	public final static String getLastDayInMonth(int year, int month) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.add(Calendar.DAY_OF_MONTH, -1);
		return df.format(c.getTime());
	}

	/**
	 * 获取指定年月的首日和末日(yyyy-MM-dd)
	 * @author dyh 2011-12-01
	 * @param year 年份
	 * @prama month 月份
	 * @return
	 */
	public final static String[] getDatesInMonth(int year, int month) {
		String dates[] = { getFirstDayInMonth(year, month), getLastDayInMonth(year, month) };
		return dates;
	}

	/**
	 * 输入起止月份，根据年月相同情况，合并相同的年月，分别显示最少的信息。比如：年相同，则不显示年
	 * @author dyh 2011-12-23
	 * @param startmonth YYYY-MM
	 * @param endmonth YYYY-MM
	 * @param showYear 如果起止月份的年相同，是否显示年
	 * @return YYYY年M月
	 */
	public final static String getStartAndEndMonth(String startmonth, String endmonth, boolean showYear) {
		if (startmonth == null || endmonth == null || startmonth.length() != 7 || endmonth.length() != 7)
			return "";
		int startyear = MyFormat.formatInt(startmonth.substring(0, 4), 0);
		int startmon = MyFormat.formatInt(startmonth.substring(5, 7), 0);
		int endyear = MyFormat.formatInt(endmonth.substring(0, 4), 0);
		int endmon = MyFormat.formatInt(endmonth.substring(5, 7), 0);
		StringBuilder dateOK = new StringBuilder();
		if (startyear == endyear) {// 如果年相同
			if (showYear) {
				dateOK.append(String.valueOf(startyear));
				dateOK.append("年");
			}
			if (startmon == endmon) {
				dateOK.append(String.valueOf(startmon));
				dateOK.append("月");
			} else {
				dateOK.append(String.valueOf(startmon));
				dateOK.append("月-");
				dateOK.append(String.valueOf(endmon));
				dateOK.append("月");
			}
		} else {
			dateOK.append(String.valueOf(startyear));
			dateOK.append("年");
			dateOK.append(String.valueOf(startmon));
			dateOK.append("月-");
			dateOK.append(String.valueOf(endyear));
			dateOK.append("年");
			dateOK.append(String.valueOf(endmon));
			dateOK.append("月");
		}
		return dateOK.toString();
	}

	/**
	 * 输入起止月份，根据年月相同情况，合并相同的年月，分别显示最少的信息。比如：年相同，则不显示年
	 * @author dyh 2011-12-23
	 * @param startmonth YYYY-MM
	 * @param endmonth YYYY-MM
	 * @param showYear 如果起止月份的年相同，是否显示年
	 * @return YYYY年M月
	 */
	public final static String getStartAndEndMonth(String startmonth, String endmonth) {
		return getStartAndEndMonth(startmonth, endmonth, true);
	}

	/**
	 * 输入日期，输出该日为第几季度
	 * @author dyh 2011-01-18
	 * @param date 日期(YYYY-MM-DD)
	 * @return
	 */
	public final static int getQuarter(String date) {
		if (MyFormat.isStrNull(date) || date.length() != 10)
			return -1;
		else {
			return (MyFormat.getMonthInt(date) - 1) / 3 + 1;
		}
	}

	/******************* 1.5 日期方法结束：季 ***************************/

	/******************* 1.6 日期方法开始：年 ***************************/
	/**
	 * 获取今年
	 * @author dyh 2009-10-24
	 * @return YYYY
	 */
	public final static int getYear() {
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		return Integer.parseInt(year);
	}

	/**
	 * 输入年份位数(4位或2位)，输出今年
	 * @author dyh 2009-10-24
	 * @param n (4位或2位),默认4位
	 * @return 根据n位数(4位或2位)输出今年
	 */
	public final static String getYear(int n) {
		if (!(n == 2 || n == 4))
			n = 4;
		Date date = new Date();
		String year = "";
		for (int i = 0; i < n; i++)
			year += "y";
		SimpleDateFormat df = new SimpleDateFormat(year);
		return df.format(date);
	}

	/**
	 * 输入日期，取出年份
	 * @author dyh 2011-01-24
	 * @param date YYYY-MM-DD或YYYY-MM
	 * @return 年份
	 */
	public final static int getYear(String date) {
		return MyFormat.formatInt(date.substring(0, 4));
	}

	/**
	 * 判断是否为闰年
	 * @author dyh 2011-01-18
	 * @param year(YYYY)
	 * @return
	 */
	public final static boolean isLeapYear(int year) {
		return (year % 4 == 0 && year % 100 != 0 || year % 400 == 0);
	}

	/**
	 * 判断是否为闰年
	 * @author dyh 2011-01-18
	 * @param year(YYYY)
	 * @return
	 */
	public final static boolean isLeapYear(String year) {
		return isLeapYear(MyFormat.formatInt(year));
	}

	/**
	 * 输入年份，输出该年全年天数
	 * @author dyh 2010-04-28
	 * @param year 年份
	 * @return
	 */
	public final static int getDaysInFullYear(int year) {
		Calendar c = Calendar.getInstance();
		c.set(year, Calendar.DECEMBER, 31);
		return c.get(Calendar.DAY_OF_YEAR);
	}

	/**
	 * 输入日期，输出该日为一年中的第几天
	 * @author dyh 2011-01-18
	 * @param date 日期(YYYY-MM-DD)
	 * @return
	 */
	public final static int getDaysInYear(String date) {
		Calendar c = Calendar.getInstance();
		c.set(MyFormat.getYear(date), MyFormat.getMonthInt(date) - 1, MyFormat.getDay(date));
		return c.get(Calendar.DAY_OF_YEAR);
	}

	/**
	 * 获取去年年份
	 * @author YOUCL 2011-1-18
	 * @return
	 */
	public final static String getLastYear() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) - 1);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		return df.format(calendar.getTime());
	}

	/**
	 * 获取指定年月按月统计的起止日期二维数组,只能统计相邻两年 如输入2010,1,2010,2返回{{'2010-01-01','2010-01-31'},{'2010-02-01','2010-02-28'}}
	 * @author ycl 2010-05-28
	 * @param startyear
	 * @param startmonth
	 * @param endyear
	 * @param endmonth
	 * @return
	 */
	public final static String[][] getStartEndDayInYear(int startyear, int startmonth, int endyear, int endmonth) {
		String arr[][] = null;
		int row = getSpaceMonthInYear(startyear, startmonth, endyear, endmonth);
		arr = new String[row][];
		int step = 0;
		if (endyear > startyear) {
			for (int i = startmonth; i <= 12; i++) {
				String dates[] = new String[2];
				getDayByMonthInYear(startyear, i, dates);
				arr[step] = dates;
				step++;
			}
			for (int i = 1; i <= endmonth; i++) {
				String dates[] = new String[2];
				getDayByMonthInYear(endyear, i, dates);
				arr[step] = dates;
				step++;
			}
		} else {
			for (int i = startmonth; i <= endmonth; i++) {
				String dates[] = new String[2];
				getDayByMonthInYear(startyear, i, dates);
				arr[step] = dates;
				step++;
			}
		}
		return arr;
	}
	
	/**
	 * 输入N位字符串型数字，输出高位不带0的字符串型数字 如：输入“01”，则输出“1”；输入“12”，则输出“12”
	 * @author dyh 2009-10-24
	 * @param x N位字符串型数字
	 * @return 高位不带0的字符串型数字
	 */
	public final static String getRealNumber(String x) {
		String result = x;
		if (MyFormat.isStrNull(result))
			return "";
		while (result.length() > 1 && result.startsWith("0")) {
			result = result.substring(1);
			getRealNumber(result);
		}
		return result;
	}
	
	/**
	 * 输入数字，输出长度为n的字符串型数字（高位补0）
	 * @author dyh 2009-10-24
	 * @param x 数字
	 * @return 高位带0的字符串型数字
	 */
	public final static String getFixedNumber(int x, int n) {
		String xx = String.valueOf(x);
		int length = xx.length();
		StringBuilder s = new StringBuilder();
		for (int i = length; i < n; i++) {
			s.append("0");
		}
		s.append(xx);
		return s.toString();
	}
	
	public static void main(String[] args) {
		//String month = MyFormat.getPrevPrevMonth();//上上个月
		//如果本月过了15号，就算上个月
		//if (MyFormat.isHalfPassMonth()){
		//String today = MyFormat.getToday();
			//String month = MyFormat.getPrevMonth();
		//}
		
		//System.out.println(today);
		//System.out.println(month);
		String lastd_start = MyFormat.getYesterday();
		String lastd_end = MyFormat.getYesterday();
		
		
		String lastw_start = "";
		String lastw_end = "";
		String[] weeks = MyFormat.getWeek(null, -1);
		lastw_start = weeks[0];
		lastw_end = weeks[1];
		
		String lastm_start = MyFormat.getFirstDayInLastMonth();
		String lastm_end = MyFormat.getLastDayInLastMonth();
		System.out.println(lastd_start);
		System.out.println(lastd_end);
		System.out.println(lastw_start);
		System.out.println(lastw_end);
		System.out.println(lastm_start);
		System.out.println(lastm_end);
	}
	
}
