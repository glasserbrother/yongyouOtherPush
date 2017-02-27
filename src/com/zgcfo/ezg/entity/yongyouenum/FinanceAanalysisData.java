package com.zgcfo.ezg.entity.yongyouenum;

public enum FinanceAanalysisData {

	库存现金(1,"库存现金","end_jf_balance","subject_id",true,"balance"),
	管理费用下业务招待费(2,"管理费用-业务招待费","year_jf_accumulate","subject_id",true,"balance"),	
	利润表下年营业收入(3,"一、营业收入","year_cash","item",false,"income"),
	利润表下月营业收入(4,"一、营业收入","month_cash","item",false,"income"),
	管理费用下福利费(5,"管理费用-福利费","year_jf_accumulate","subject_id",true,"balance"),
	管理费用下研究费用下福利费(6,"管理费用-研究费用-福利费","year_jf_accumulate","subject_id",true,"balance"),
	管理费用下管理人员职工薪酬(7,"管理费用-管理人员职工薪酬","year_jf_accumulate","subject_id",true,"balance"),
	管理费用下研究费用下研发人员职工薪酬(8,"管理费用-研究费用-研发人员职工薪酬","year_jf_accumulate","subject_id",true,"balance"),
	销售费用下销售人员职工薪酬(9,"销售费用-销售人员职工薪酬","year_jf_accumulate","subject_id",true,"balance"),
	销售费用下广告费(10,"销售费用-广告费","year_jf_accumulate","subject_id",true,"balance"),
	销售费用下业务宣传费(11,"销售费用-业务宣传费","year_jf_accumulate","subject_id",true,"balance"),
	利润表季报表下第一季度营业收入(12,"一、营业收入","one_quarter_cash","item",false,"incomeQuarter"),
	利润表季报表下第二季度营业收入(13,"一、营业收入","two_quarter_cash","item",false,"incomeQuarter"),
	利润表季报表下第三季度营业收入(14,"一、营业收入","three_quarter_cash","item",false,"incomeQuarter"),
	利润表季报表下第四季度营业收入(15,"一、营业收入","four_quarter_cash","item",false,"incomeQuarter"),
	应交税费下应交个人所得税(16,"应交税费-应交个人所得税","year_df_accumulate","subject_id",true,"balance"),
	无形资产当月(17,"无形资产","asset_end_balance","asset",false,"balanceReport"),
	无形资产前一月(18,"无形资产","asset_end_balance","asset",false,"balanceReport"),
	实收资本当月(19,"实收资本（或股本）","equity_end_balance","equity",false,"balanceReport"),
	实收资本前一月(20,"实收资本（或股本）","equity_end_balance","equity",false,"balanceReport"),
	银行存款(21,"银行存款","end_jf_balance","subject_id",true,"balance"),
	预收账款(22,"预收账款","equity_end_balance","equity",false,"balanceReport"),
	应缴税费(23,"应缴税费","","",false,""),
	应缴当季度税费(24,"应缴税费","","",false,"");
	
	private Integer value;
	
	private String targetkey;
	
	private String targetValue;
	private String key;
	
	private boolean isBySbj;
	
	private String targetReport;
	
    private FinanceAanalysisData(int value,String targetValue,String key,String targetkey,boolean isBySbjName,String targerReport) {
        this.value = value; 
        this.targetkey = targetkey;
        this.targetValue = targetValue; 
        this.key = key;
        this.isBySbj = isBySbjName; 
        this.targetReport = targerReport;
    } 
    
    public int getValue() {
    	
        return value;
    }

	public String getTargetkey() {
		return targetkey;
	}

	public String getTargetValue() {
		return targetValue;
	}

	public String getKey() {
		return key;
	}

	public boolean isBySbj() {
		return isBySbj;
	}

	public String getTargetReport() {
		return targetReport;
	}
    
}
