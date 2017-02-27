package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class IncomeReport implements Serializable{
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
	
	/***
	 * 上年同期累计金额
	 * */
	private Double lastYearCashBD;
	
	/**
	 * 本月金额
	 * */
	private Double monthCash;
	
	/**
	 * 本月金额
	 * **/
	private Double monthCashBD;
	

	
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
	
	private String accountBookId;
	
	private String uniqueId;
	
	private String incomeReportId;
	
	private String period;

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

	public Double getLastYearCashBD() {
		return lastYearCashBD;
	}

	public void setLastYearCashBD(Double lastYearCashBD) {
		this.lastYearCashBD = lastYearCashBD;
	}

	public Double getMonthCash() {
		return monthCash;
	}

	public void setMonthCash(Double monthCash) {
		this.monthCash = monthCash;
	}

	public Double getMonthCashBD() {
		return monthCashBD;
	}

	public void setMonthCashBD(Double monthCashBD) {
		this.monthCashBD = monthCashBD;
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

	public String getAccountBookId() {
		return accountBookId;
	}

	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}

	public String getIncomeReportId() {
		return incomeReportId;
	}

	public void setIncomeReportId(String incomeReportId) {
		this.incomeReportId = incomeReportId;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}
	
	
}
