package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class DetailAccountReport implements Serializable{
	
	private String voucherId;
	
	private String period; 
	
	private String voucherWordNo; 
	
	private String summary; 
	
	private String direction; 
	
	private Double jfBalance; 
	
	private Double dfBalance; 
	
	private String balance;
	
	private Double jfBalanceOrg;
	
	private Double dfBalanceOrg; 
	
	private Double balanceOrg; 
	
	private String exchangeRate; 
	
	private String jfNum; 
	
	private String dfNum;
	
	private String balanceNum; 
	
	private Double jfPrice;
	
	private Double dfPrice; 
	
	private Double balancePrice;
	
	private String subjectId;
	
	private String subjectName; 
	
	private String subjectCode; 
	
	private String firstName; 
	
	private String pageNoView; 
	
	private String groupCount; 
	
	private String measuringUnit; 
	
	private String currSymbol; 
	
	private String isAssistantAccountSelf;
	
	private String leaf; 
	
	private String unitName;

	
	private String startPeriod;//开始期间
	
	private String endPeriod;//结束期间
	
	private String accountBookId;//账簿Id
	
	private String uniqueId;  //accountBookId+startPeriod+endPeriod+下标
	
	private String detailAccountId;//主键ID uuid

	private String ezgSubjectId;
	
	
	public String getVoucherId() {
		return voucherId;
	}

	public void setVoucherId(String voucherId) {
		this.voucherId = voucherId;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public String getVoucherWordNo() {
		return voucherWordNo;
	}

	public void setVoucherWordNo(String voucherWordNo) {
		this.voucherWordNo = voucherWordNo;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public Double getJfBalance() {
		return jfBalance;
	}

	public void setJfBalance(Double jfBalance) {
		this.jfBalance = jfBalance;
	}

	public Double getDfBalance() {
		return dfBalance;
	}

	public void setDfBalance(Double dfBalance) {
		this.dfBalance = dfBalance;
	}

	public String getBalance() {
		return balance;
	}

	public void setBalance(String balance) {
		this.balance = balance;
	}

	public Double getJfBalanceOrg() {
		return jfBalanceOrg;
	}

	public void setJfBalanceOrg(Double jfBalanceOrg) {
		this.jfBalanceOrg = jfBalanceOrg;
	}

	public Double getDfBalanceOrg() {
		return dfBalanceOrg;
	}

	public void setDfBalanceOrg(Double dfBalanceOrg) {
		this.dfBalanceOrg = dfBalanceOrg;
	}

	public Double getBalanceOrg() {
		return balanceOrg;
	}

	public void setBalanceOrg(Double balanceOrg) {
		this.balanceOrg = balanceOrg;
	}

	public String getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(String exchangeRate) {
		this.exchangeRate = exchangeRate;
	}

	public String getJfNum() {
		return jfNum;
	}

	public void setJfNum(String jfNum) {
		this.jfNum = jfNum;
	}

	public String getDfNum() {
		return dfNum;
	}

	public void setDfNum(String dfNum) {
		this.dfNum = dfNum;
	}

	public String getBalanceNum() {
		return balanceNum;
	}

	public void setBalanceNum(String balanceNum) {
		this.balanceNum = balanceNum;
	}

	public Double getJfPrice() {
		return jfPrice;
	}

	public void setJfPrice(Double jfPrice) {
		this.jfPrice = jfPrice;
	}

	public Double getDfPrice() {
		return dfPrice;
	}

	public void setDfPrice(Double dfPrice) {
		this.dfPrice = dfPrice;
	}

	public Double getBalancePrice() {
		return balancePrice;
	}

	public void setBalancePrice(Double balancePrice) {
		this.balancePrice = balancePrice;
	}

	public String getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	public String getSubjectName() {
		return subjectName;
	}

	public void setSubjectName(String subjectName) {
		this.subjectName = subjectName;
	}

	public String getSubjectCode() {
		return subjectCode;
	}

	public void setSubjectCode(String subjectCode) {
		this.subjectCode = subjectCode;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getPageNoView() {
		return pageNoView;
	}

	public void setPageNoView(String pageNoView) {
		this.pageNoView = pageNoView;
	}

	public String getGroupCount() {
		return groupCount;
	}

	public void setGroupCount(String groupCount) {
		this.groupCount = groupCount;
	}

	public String getMeasuringUnit() {
		return measuringUnit;
	}

	public void setMeasuringUnit(String measuringUnit) {
		this.measuringUnit = measuringUnit;
	}

	public String getCurrSymbol() {
		return currSymbol;
	}

	public void setCurrSymbol(String currSymbol) {
		this.currSymbol = currSymbol;
	}

	public String getIsAssistantAccountSelf() {
		return isAssistantAccountSelf;
	}

	public void setIsAssistantAccountSelf(String isAssistantAccountSelf) {
		this.isAssistantAccountSelf = isAssistantAccountSelf;
	}

	public String getLeaf() {
		return leaf;
	}

	public void setLeaf(String leaf) {
		this.leaf = leaf;
	}

	public String getUnitName() {
		return unitName;
	}

	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}

	public String getStartPeriod() {
		return startPeriod;
	}

	public void setStartPeriod(String startPeriod) {
		this.startPeriod = startPeriod;
	}

	public String getEndPeriod() {
		return endPeriod;
	}

	public void setEndPeriod(String endPeriod) {
		this.endPeriod = endPeriod;
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

	public String getDetailAccountId() {
		return detailAccountId;
	}

	public void setDetailAccountId(String detailAccountId) {
		this.detailAccountId = detailAccountId;
	}

	public String getEzgSubjectId() {
		return ezgSubjectId;
	}

	public void setEzgSubjectId(String ezgSubjectId) {
		this.ezgSubjectId = ezgSubjectId;
	} 
	
	
}
