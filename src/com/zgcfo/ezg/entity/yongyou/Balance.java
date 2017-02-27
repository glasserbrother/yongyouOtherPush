package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class Balance implements Serializable {
	
	
	private String subjectType; 
	
	private String subjectNo; 
	
	private String subjectId; 

	private String subjectName;

	 

	private Double initJfBalance;

	private Double initDfBalance;

	private Double initYearJfBalance;

	private Double initYearDfBalance;
	
	 
	
	private Double currentPeriodJfBalance; 

	private Double currentPeriodDfBalance; 
	
	private Double currentPeriodJfActualBalance;

	private Double currentPeriodDfActualBalance;

	private Double yearJfBalance;

	private Double yearDfBalance;

	private Double yearJfActualBalance;

	private Double yearDfActualBalance; 

	private Double yearJfAccumulate;

	private Double yearDfAccumulate; 

	 

	private Double endJfbalance; 

	private Double endDfbalance;

	private Double endYearJfbalance; 

	private Double initYearBalance; 

	private Double endYearDfbalance;
	
	 
	  


	private Boolean leaf; 

	private Boolean hasAssistantAccount;

	private String assistantAccount;

	private String parentNo; 

	
	
	
	 
	private String direction;

	private String period; 

	private Double endbalance;

	private Double initPrice; 

	
	private Double endPrice; 

	private String unit; 

	private String currency; 
	
	private Double initJfBalanceOrg;

	private Double initDfBalanceOrg;

	private Double currentPeriodJfBalanceOrg;

	private Double currentPeriodDfBalanceOrg;

	private Double endJfbalanceOrg;

	private Double endDfbalanceOrg;

	private Boolean endBException;

	private String assistantNo;

	private String summary; 

	private Integer balanceType;//余额表类型0.余额表1.数量余额表 3.外币余额表
	
	private String startPeriod;//开始期间
	
	private String endPeriod;//结束期间
	
	private String accountBookId;//账簿Id
	
	private String uniqueId;  //accountBookId+startPeriod+endPeriod+下标
	
	private String balanceId;//主键ID uuid

	public String getSubjectType() {
		return subjectType;
	}

	public void setSubjectType(String subjectType) {
		this.subjectType = subjectType;
	}

	public String getSubjectNo() {
		return subjectNo;
	}

	public void setSubjectNo(String subjectNo) {
		this.subjectNo = subjectNo;
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

	public Double getInitJfBalance() {
		return initJfBalance;
	}

	public void setInitJfBalance(Double initJfBalance) {
		this.initJfBalance = initJfBalance;
	}

	public Double getInitDfBalance() {
		return initDfBalance;
	}

	public void setInitDfBalance(Double initDfBalance) {
		this.initDfBalance = initDfBalance;
	}

	public Double getInitYearJfBalance() {
		return initYearJfBalance;
	}

	public void setInitYearJfBalance(Double initYearJfBalance) {
		this.initYearJfBalance = initYearJfBalance;
	}

	public Double getInitYearDfBalance() {
		return initYearDfBalance;
	}

	public void setInitYearDfBalance(Double initYearDfBalance) {
		this.initYearDfBalance = initYearDfBalance;
	}

	public Double getCurrentPeriodJfBalance() {
		return currentPeriodJfBalance;
	}

	public void setCurrentPeriodJfBalance(Double currentPeriodJfBalance) {
		this.currentPeriodJfBalance = currentPeriodJfBalance;
	}

	public Double getCurrentPeriodDfBalance() {
		return currentPeriodDfBalance;
	}

	public void setCurrentPeriodDfBalance(Double currentPeriodDfBalance) {
		this.currentPeriodDfBalance = currentPeriodDfBalance;
	}

	public Double getCurrentPeriodJfActualBalance() {
		return currentPeriodJfActualBalance;
	}

	public void setCurrentPeriodJfActualBalance(Double currentPeriodJfActualBalance) {
		this.currentPeriodJfActualBalance = currentPeriodJfActualBalance;
	}

	public Double getCurrentPeriodDfActualBalance() {
		return currentPeriodDfActualBalance;
	}

	public void setCurrentPeriodDfActualBalance(Double currentPeriodDfActualBalance) {
		this.currentPeriodDfActualBalance = currentPeriodDfActualBalance;
	}

	public Double getYearJfBalance() {
		return yearJfBalance;
	}

	public void setYearJfBalance(Double yearJfBalance) {
		this.yearJfBalance = yearJfBalance;
	}

	public Double getYearDfBalance() {
		return yearDfBalance;
	}

	public void setYearDfBalance(Double yearDfBalance) {
		this.yearDfBalance = yearDfBalance;
	}

	public Double getYearJfActualBalance() {
		return yearJfActualBalance;
	}

	public void setYearJfActualBalance(Double yearJfActualBalance) {
		this.yearJfActualBalance = yearJfActualBalance;
	}

	public Double getYearDfActualBalance() {
		return yearDfActualBalance;
	}

	public void setYearDfActualBalance(Double yearDfActualBalance) {
		this.yearDfActualBalance = yearDfActualBalance;
	}

	public Double getYearJfAccumulate() {
		return yearJfAccumulate;
	}

	public void setYearJfAccumulate(Double yearJfAccumulate) {
		this.yearJfAccumulate = yearJfAccumulate;
	}

	public Double getYearDfAccumulate() {
		return yearDfAccumulate;
	}

	public void setYearDfAccumulate(Double yearDfAccumulate) {
		this.yearDfAccumulate = yearDfAccumulate;
	}

	public Double getEndJfbalance() {
		return endJfbalance;
	}

	public void setEndJfbalance(Double endJfbalance) {
		this.endJfbalance = endJfbalance;
	}

	public Double getEndDfbalance() {
		return endDfbalance;
	}

	public void setEndDfbalance(Double endDfbalance) {
		this.endDfbalance = endDfbalance;
	}

	public Double getEndYearJfbalance() {
		return endYearJfbalance;
	}

	public void setEndYearJfbalance(Double endYearJfbalance) {
		this.endYearJfbalance = endYearJfbalance;
	}

	public Double getInitYearBalance() {
		return initYearBalance;
	}

	public void setInitYearBalance(Double initYearBalance) {
		this.initYearBalance = initYearBalance;
	}

	public Double getEndYearDfbalance() {
		return endYearDfbalance;
	}

	public void setEndYearDfbalance(Double endYearDfbalance) {
		this.endYearDfbalance = endYearDfbalance;
	}

	public Boolean getLeaf() {
		return leaf;
	}

	public void setLeaf(Boolean leaf) {
		this.leaf = leaf;
	}

	public Boolean getHasAssistantAccount() {
		return hasAssistantAccount;
	}

	public void setHasAssistantAccount(Boolean hasAssistantAccount) {
		this.hasAssistantAccount = hasAssistantAccount;
	}

	public String getAssistantAccount() {
		return assistantAccount;
	}

	public void setAssistantAccount(String assistantAccount) {
		this.assistantAccount = assistantAccount;
	}

	public String getParentNo() {
		return parentNo;
	}

	public void setParentNo(String parentNo) {
		this.parentNo = parentNo;
	}

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public Double getEndbalance() {
		return endbalance;
	}

	public void setEndbalance(Double endbalance) {
		this.endbalance = endbalance;
	}

	public Double getInitPrice() {
		return initPrice;
	}

	public void setInitPrice(Double initPrice) {
		this.initPrice = initPrice;
	}

	public Double getEndPrice() {
		return endPrice;
	}

	public void setEndPrice(Double endPrice) {
		this.endPrice = endPrice;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public Double getInitJfBalanceOrg() {
		return initJfBalanceOrg;
	}

	public void setInitJfBalanceOrg(Double initJfBalanceOrg) {
		this.initJfBalanceOrg = initJfBalanceOrg;
	}

	public Double getInitDfBalanceOrg() {
		return initDfBalanceOrg;
	}

	public void setInitDfBalanceOrg(Double initDfBalanceOrg) {
		this.initDfBalanceOrg = initDfBalanceOrg;
	}

	public Double getCurrentPeriodJfBalanceOrg() {
		return currentPeriodJfBalanceOrg;
	}

	public void setCurrentPeriodJfBalanceOrg(Double currentPeriodJfBalanceOrg) {
		this.currentPeriodJfBalanceOrg = currentPeriodJfBalanceOrg;
	}

	public Double getCurrentPeriodDfBalanceOrg() {
		return currentPeriodDfBalanceOrg;
	}

	public void setCurrentPeriodDfBalanceOrg(Double currentPeriodDfBalanceOrg) {
		this.currentPeriodDfBalanceOrg = currentPeriodDfBalanceOrg;
	}

	public Double getEndJfbalanceOrg() {
		return endJfbalanceOrg;
	}

	public void setEndJfbalanceOrg(Double endJfbalanceOrg) {
		this.endJfbalanceOrg = endJfbalanceOrg;
	}

	public Double getEndDfbalanceOrg() {
		return endDfbalanceOrg;
	}

	public void setEndDfbalanceOrg(Double endDfbalanceOrg) {
		this.endDfbalanceOrg = endDfbalanceOrg;
	}

	public Boolean getEndBException() {
		return endBException;
	}

	public void setEndBException(Boolean endBException) {
		this.endBException = endBException;
	}

	public String getAssistantNo() {
		return assistantNo;
	}

	public void setAssistantNo(String assistantNo) {
		this.assistantNo = assistantNo;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public Integer getBalanceType() {
		return balanceType;
	}

	public void setBalanceType(Integer balanceType) {
		this.balanceType = balanceType;
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

	public String getBalanceId() {
		return balanceId;
	}

	public void setBalanceId(String balanceId) {
		this.balanceId = balanceId;
	}

	
}
