package com.zgcfo.ezg.app.constant;

import java.util.HashMap;
import java.util.Map;

public class YongYouConstants {
	
	public static  Map<Integer, byte[]> YYMAP = new HashMap<Integer, byte[]>();
	public static  Map<String, Integer> YYOtherMAP = new HashMap<String, Integer>();
	public static final String CASH_REPORT_STRING = "CashReport";
	public static final String INCOME_REPORT_STRING = "IncomeReport";
	public static final String BALANCE_REPORT_STRING = "BalanceReport";
	public static final String SUBJECT_STRING = "Subject";
	public static final String BALANCE_STRING = "Balance";
	public static final String INCOME_QUARTER_REPORT_STRING = "IncomeQuarterReport";
	public static final String TAX_REPORT_STRING = "TaxReport";
	public static final String DETAIL_ACCOUNT_REPORT_STRING = "DetailAccountReport";
	
	public static final byte[] CASH_REPORT_BYTE = CASH_REPORT_STRING.getBytes();
	public static final byte[] INCOME_REPORT_BYTE = INCOME_REPORT_STRING.getBytes();
	public static final byte[] BALANCE_REPORT_BYTE = BALANCE_REPORT_STRING.getBytes();
	public static final byte[] SUBJECT_BYTE = SUBJECT_STRING.getBytes();
	public static final byte[] BALANCE_BYTE = BALANCE_STRING.getBytes();
	public static final byte[] INCOME_QUARTER_REPORT_BYTE = INCOME_QUARTER_REPORT_STRING.getBytes();
	public static final byte[] TAX_REPORT_BYTE = TAX_REPORT_STRING.getBytes();
	
	public static final byte[] DETAIL_ACCOUNT_REPORT_BYTE = DETAIL_ACCOUNT_REPORT_STRING.getBytes();
	
	public static final int CASH_REPORT_INT = 0;
	public static final int INCOME_REPORT_INT = 1;
	public static final int BALANCE_REPORT_INT = 2;
	public static final int SUBJECT_INT = 3;
	public static final int BALANCE_INT = 4;
	public static final int INCOME_QUARTER_REPORT_INT = 5;
	public static final int TAX_REPORT_INT = 6;
	public static final int DETAIL_ACCOUNT_REPORT_INT = 7;
	
	static {
		YYMAP.put(new Integer(CASH_REPORT_INT), CASH_REPORT_BYTE);
		YYMAP.put(new Integer(INCOME_REPORT_INT), INCOME_REPORT_BYTE);
		YYMAP.put(new Integer(BALANCE_REPORT_INT), BALANCE_REPORT_BYTE);
		YYMAP.put(new Integer(SUBJECT_INT), SUBJECT_BYTE);
		YYMAP.put(new Integer(BALANCE_INT), BALANCE_BYTE);
		YYMAP.put(new Integer(INCOME_QUARTER_REPORT_INT), INCOME_QUARTER_REPORT_BYTE);
		YYMAP.put(new Integer(TAX_REPORT_INT), TAX_REPORT_BYTE);
		YYMAP.put(new Integer(DETAIL_ACCOUNT_REPORT_INT), DETAIL_ACCOUNT_REPORT_BYTE);
		
		
		YYOtherMAP.put(CASH_REPORT_STRING, CASH_REPORT_INT);
		YYOtherMAP.put(INCOME_REPORT_STRING, INCOME_REPORT_INT);
		YYOtherMAP.put(BALANCE_REPORT_STRING, BALANCE_REPORT_INT);
		YYOtherMAP.put(SUBJECT_STRING, SUBJECT_INT);
		YYOtherMAP.put(BALANCE_STRING, BALANCE_INT);
		YYOtherMAP.put(INCOME_QUARTER_REPORT_STRING, INCOME_QUARTER_REPORT_INT);
		YYOtherMAP.put(TAX_REPORT_STRING, TAX_REPORT_INT);
		YYOtherMAP.put(DETAIL_ACCOUNT_REPORT_STRING, DETAIL_ACCOUNT_REPORT_INT);
		
	}
	
	
	public static void main(String[] args) {
		for(Integer key: YYMAP.keySet()){
			System.out.println("key:"+key+" vallue:"+YYMAP.get(key));
		}
	}
	

	
	
}
