package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

/**
 * 资产负债表
 * 
 * */
public class BalanceReport implements Serializable{
	
	/**
	 * 资产项名称
	 * */
	private String asset;

	/**
	 * 资产项行次
	 * */
	private Integer assetRow;
	
	/**
	 * 资产项期末余额
	 * */
	private Double assetEndBalance;
	
	/**
	 * 资产项年初余额
	 * */
	private Double assetEndBalanceBD;
	
	/**
	 * 资产项年初余额
	 * */
	private Double assetYearInitBalance;
	
	/**
	 * 资产项年初余额
	 * */
	private Double assetYearInitBalanceBD;
	
	/**
	 * 负债项名称
	 * */
	private String equity;
	
	/**
	 * 负债项行次
	 * */
	private String equityRow;
	
	/**
	 * 负债项期末余额
	 * */
	private Double equityEndBalance;
	
	/**
	 * 负债项期末余额
	 * */
	private Double equityEndBalanceBD;
	
	/**
	 * 负债项年初余额
	 * */
	private Double equityYearInitBalance;
	
	/**
	 * 负债项年初余额
	 * */
	private Double equityYearInitBalanceBD;

	
	
	/**
	 * 无意义
	 * */
	private String tag;

	/**
	 * 折叠到第n行里 n：从上往下数的行编号，不是显示的行次
	 * */
	private String fold;
	
	/**
	 * ""为显示，非空不显示
	 * */
	private String foldFlag;
	
	/**
	 * 资产项取数说明
	 * */
	private String fomularDetailOne;
	
	/**
	 * 负债项取数说明
	 * */
	private String fomularDetailTwo;

	private String period;
	
	private String accountBookId;
	
	private String unique_id;  //accountBookId+period+下标
	
	private String balanceReportId;//主键ID uuid
	
	
	
	public String getAsset() {
		return asset;
	}

	public void setAsset(String asset) {
		this.asset = asset;
	}

	public Integer getAssetRow() {
		return assetRow;
	}

	public void setAssetRow(Integer assetRow) {
		this.assetRow = assetRow;
	}

	public Double getAssetEndBalance() {
		return assetEndBalance;
	}

	public void setAssetEndBalance(Double assetEndBalance) {
		this.assetEndBalance = assetEndBalance;
	}

	public Double getAssetEndBalanceBD() {
		return assetEndBalanceBD;
	}

	public void setAssetEndBalanceBD(Double assetEndBalanceBD) {
		this.assetEndBalanceBD = assetEndBalanceBD;
	}

	public Double getAssetYearInitBalance() {
		return assetYearInitBalance;
	}

	public void setAssetYearInitBalance(Double assetYearInitBalance) {
		this.assetYearInitBalance = assetYearInitBalance;
	}

	public Double getAssetYearInitBalanceBD() {
		return assetYearInitBalanceBD;
	}

	public void setAssetYearInitBalanceBD(Double assetYearInitBalanceBD) {
		this.assetYearInitBalanceBD = assetYearInitBalanceBD;
	}

	public String getEquity() {
		return equity;
	}

	public void setEquity(String equity) {
		this.equity = equity;
	}

	public String getEquityRow() {
		return equityRow;
	}

	public void setEquityRow(String equityRow) {
		this.equityRow = equityRow;
	}

	public Double getEquityEndBalance() {
		return equityEndBalance;
	}

	public void setEquityEndBalance(Double equityEndBalance) {
		this.equityEndBalance = equityEndBalance;
	}

	public Double getEquityEndBalanceBD() {
		return equityEndBalanceBD;
	}

	public void setEquityEndBalanceBD(Double equityEndBalanceBD) {
		this.equityEndBalanceBD = equityEndBalanceBD;
	}

	public Double getEquityYearInitBalance() {
		return equityYearInitBalance;
	}

	public void setEquityYearInitBalance(Double equityYearInitBalance) {
		this.equityYearInitBalance = equityYearInitBalance;
	}

	public Double getEquityYearInitBalanceBD() {
		return equityYearInitBalanceBD;
	}

	public void setEquityYearInitBalanceBD(Double equityYearInitBalanceBD) {
		this.equityYearInitBalanceBD = equityYearInitBalanceBD;
	}


	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getFold() {
		return fold;
	}

	public void setFold(String fold) {
		this.fold = fold;
	}

	public String getFoldFlag() {
		return foldFlag;
	}

	public void setFoldFlag(String foldFlag) {
		this.foldFlag = foldFlag;
	}

	public String getFomularDetailOne() {
		return fomularDetailOne;
	}

	public void setFomularDetailOne(String fomularDetailOne) {
		this.fomularDetailOne = fomularDetailOne;
	}

	public String getFomularDetailTwo() {
		return fomularDetailTwo;
	}

	public void setFomularDetailTwo(String fomularDetailTwo) {
		this.fomularDetailTwo = fomularDetailTwo;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public String getAccountBookId() {
		return accountBookId;
	}

	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}

	public String getUnique_id() {
		return unique_id;
	}

	public void setUnique_id(String unique_id) {
		this.unique_id = unique_id;
	}

	public String getBalanceReportId() {
		return balanceReportId;
	}

	public void setBalanceReportId(String balanceReportId) {
		this.balanceReportId = balanceReportId;
	}
	public static void main(String[] args) {
		for(int i=0;i<5;i++){
			System.out.println(i);
		}
	}
}
