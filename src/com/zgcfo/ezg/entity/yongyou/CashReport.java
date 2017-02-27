package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class CashReport implements Serializable{
	
	private String item;
	
	private String row;
	
	private Double yearCash;
	
	private Double yearCashBD;
	
	private Double lastYearCash;
	
	private Double lastYearCashBD;

	private Double monthCash;
	
	private Double monthCashBD;
	
	private String tag;
	
	private String fold;
	
	private String foldFlag;
	
	private String fomularDetail;
	
	private String editTag;
	
	private String accountBookId;
	
	private String uniqueId;
	
	private String period;
	
	private String cashReportId;

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public Double getYearCash() {
		return yearCash;
	}

	public void setYearCash(Double yearCash) {
		this.yearCash = yearCash;
	}

	public Double getYearCashBD() {
		return yearCashBD;
	}

	public void setYearCashBD(Double yearCashBD) {
		this.yearCashBD = yearCashBD;
	}

	public Double getLastYearCash() {
		return lastYearCash;
	}

	public void setLastYearCash(Double lastYearCash) {
		this.lastYearCash = lastYearCash;
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

	public String getEditTag() {
		return editTag;
	}

	public void setEditTag(String editTag) {
		this.editTag = editTag;
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

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public String getCashReportId() {
		return cashReportId;
	}

	public void setCashReportId(String cashReportId) {
		this.cashReportId = cashReportId;
	}

}
