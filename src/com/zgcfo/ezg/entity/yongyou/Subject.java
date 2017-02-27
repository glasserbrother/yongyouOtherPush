package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class Subject implements Serializable{
	
	private Integer yongyouId;
	
	private String subTypeCode;
	
	private String subjectNo;
	
	private String subjectText;
	
	private String dir;
	
	private Boolean subjectLeaf;

	private String accountBookId;//账簿Id
	
	private String uniqueId;  //accountBookId+科目yongyouId+下标
	
	private String balanceId;//主键ID uuid

	public Integer getYongyouId() {
		return yongyouId;
	}

	public void setYongyouId(Integer yongyouId) {
		this.yongyouId = yongyouId;
	}

	public String getSubTypeCode() {
		return subTypeCode;
	}

	public void setSubTypeCode(String subTypeCode) {
		this.subTypeCode = subTypeCode;
	}

	public String getSubjectNo() {
		return subjectNo;
	}

	public void setSubjectNo(String subjectNo) {
		this.subjectNo = subjectNo;
	}

	public String getSubjectText() {
		return subjectText;
	}

	public void setSubjectText(String subjectText) {
		this.subjectText = subjectText;
	}

	public String getDir() {
		return dir;
	}

	public void setDir(String dir) {
		this.dir = dir;
	}

	public boolean getSubjectLeaf() {
		return subjectLeaf;
	}

	public void setSubjectLeaf(Boolean subjectLeaf) {
		this.subjectLeaf = subjectLeaf;
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
