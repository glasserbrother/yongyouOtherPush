package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class IncomeQuarterReport implements Serializable{
	

	
	/**
	 * 项目
	 * */
	private String item;
	
	/**
	 * 行次
	 * */
	private Integer row;
	
	/**
	 * 本年累计金额
	 * */
	private Double yearCash;
	
	/**
	 * 上年同期累计金额
	 * */
	private Double lastYearCash;
	
	/**
	 * 本年累计金额
	 * **/
	private Double yearCashBD;
	

	/**
	 * 上年同期累计金额
	 * */
	private Double lastYearCashBD;
	

	/**
	 * 第一季度
	 * **/
	private Double oneQuarterCash;
	
	/**
	 * 第一季度
	 * **/
	private Double oneQuarterCashBD;
	
	/**
	 * 第二季度
	 * **/
	private Double twoQuarterCash;
	
	/**
	 * 第二季度
	 * **/
	private Double twoQuarterCashBD;
	
	/**
	 * 第三季度
	 * **/
	private Double threeQuarterCash;
	
	/**
	 * 第三季度
	 * **/
	private Double threeQuarterCashBD;
	
	/**
	 * 第四季度
	 * **/
	private Double fourQuarterCash;
	
	/**
	 * 第四季度
	 * **/
	private Double fourQuarterCashBD;
	

	/**
	 * 无意义
	 * **/
	private String tag;
	
	/**
	 * 折叠到第n行里 n：从上往下数的行编号，不是显示的行次
	 * **/
	private String fold;
	
	/**
	 * ""为显示，非空不显示
	 * **/
	private String foldFlag;
	
	/**
	 * 取数说明
	 * **/
	private String fomularDetail;
	
	/**
	 * "1,2"→第一二季度置灰
	 * */
	private String colorFlag;
	
	
	private String uniqueId;
	
	private String accountBookId;
	
	private String period;
	
	private String incomeQuarterId;

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public Integer getRow() {
		return row;
	}

	public void setRow(Integer row) {
		this.row = row;
	}

	public Double getYearCash() {
		return yearCash;
	}

	public void setYearCash(Double yearCash) {
		this.yearCash = yearCash;
	}

	public Double getLastYearCash() {
		return lastYearCash;
	}

	public void setLastYearCash(Double lastYearCash) {
		this.lastYearCash = lastYearCash;
	}

	public Double getYearCashBD() {
		return yearCashBD;
	}

	public void setYearCashBD(Double yearCashBD) {
		this.yearCashBD = yearCashBD;
	}

	public Double getOneQuarterCash() {
		return oneQuarterCash;
	}

	public void setOneQuarterCash(Double oneQuarterCash) {
		this.oneQuarterCash = oneQuarterCash;
	}

	public Double getOneQuarterCashBD() {
		return oneQuarterCashBD;
	}

	public void setOneQuarterCashBD(Double oneQuarterCashBD) {
		this.oneQuarterCashBD = oneQuarterCashBD;
	}

	public Double getTwoQuarterCash() {
		return twoQuarterCash;
	}

	public void setTwoQuarterCash(Double twoQuarterCash) {
		this.twoQuarterCash = twoQuarterCash;
	}

	public Double getTwoQuarterCashBD() {
		return twoQuarterCashBD;
	}

	public void setTwoQuarterCashBD(Double twoQuarterCashBD) {
		this.twoQuarterCashBD = twoQuarterCashBD;
	}

	public Double getThreeQuarterCash() {
		return threeQuarterCash;
	}

	public void setThreeQuarterCash(Double threeQuarterCash) {
		this.threeQuarterCash = threeQuarterCash;
	}

	public Double getThreeQuarterCashBD() {
		return threeQuarterCashBD;
	}

	public void setThreeQuarterCashBD(Double threeQuarterCashBD) {
		this.threeQuarterCashBD = threeQuarterCashBD;
	}

	public Double getFourQuarterCash() {
		return fourQuarterCash;
	}

	public void setFourQuarterCash(Double fourQuarterCash) {
		this.fourQuarterCash = fourQuarterCash;
	}

	public Double getFourQuarterCashBD() {
		return fourQuarterCashBD;
	}

	public void setFourQuarterCashBD(Double fourQuarterCashBD) {
		this.fourQuarterCashBD = fourQuarterCashBD;
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

	public String getFomularDetail() {
		return fomularDetail;
	}

	public void setFomularDetail(String fomularDetail) {
		this.fomularDetail = fomularDetail;
	}

	public String getColorFlag() {
		return colorFlag;
	}

	public void setColorFlag(String colorFlag) {
		this.colorFlag = colorFlag;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}

	public String getAccountBookId() {
		return accountBookId;
	}

	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public String getIncomeQuarterId() {
		return incomeQuarterId;
	}

	public void setIncomeQuarterId(String incomeQuarterId) {
		this.incomeQuarterId = incomeQuarterId;
	}

	
	
	public Double getLastYearCashBD() {
		return lastYearCashBD;
	}

	public void setLastYearCashBD(Double lastYearCashBD) {
		this.lastYearCashBD = lastYearCashBD;
	}

	

}
