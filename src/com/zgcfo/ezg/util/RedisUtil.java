package com.zgcfo.ezg.util;

import com.zgcfo.ezg.app.constant.YongYouConstants;

public class RedisUtil {
	
	
	public static byte[] getCommondRedisKey(int paraCommond){
		byte[] redisKey = new byte[1];
		
		switch(paraCommond){
		case YongYouConstants.CASH_REPORT_INT : 
			redisKey = YongYouConstants.CASH_REPORT_BYTE;//开始导入现金流量表
			break;
		case YongYouConstants.INCOME_REPORT_INT : 
			redisKey = YongYouConstants.INCOME_REPORT_BYTE;//开始导入利润表
			break;
		case YongYouConstants.BALANCE_REPORT_INT : 
			redisKey = YongYouConstants.BALANCE_REPORT_BYTE;//开始导入资产负债表
			break;
		case YongYouConstants.SUBJECT_INT : 
			redisKey = YongYouConstants.SUBJECT_BYTE;//开始导入科目
			break;
		case YongYouConstants.BALANCE_INT : 
			redisKey = YongYouConstants.BALANCE_BYTE;//开始导入余额
			break;
		case YongYouConstants.INCOME_QUARTER_REPORT_INT :
			redisKey = YongYouConstants.INCOME_QUARTER_REPORT_BYTE;//开始导入利润季报表
			break;
		case YongYouConstants.TAX_REPORT_INT : 
			redisKey = YongYouConstants.TAX_REPORT_BYTE;//开始导入纳税统计表
			break;
		case YongYouConstants.DETAIL_ACCOUNT_REPORT_INT : 
			redisKey = YongYouConstants.DETAIL_ACCOUNT_REPORT_BYTE;//开始导入现金流量表
			break;
		default : break;
		}
		return redisKey;
	}

}
