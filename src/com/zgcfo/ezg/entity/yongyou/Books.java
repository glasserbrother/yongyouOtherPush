package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

/**
 * 易代账账簿对象
 * **/
public class Books implements Serializable{
	
	private String userName;
	
	private Integer yongyouId;
	
	private String bookName;
	
	private String taxNo;
	
	private Integer accSysId;
	
	private String accSysName;
	
	private String startPeriod;
	
	private String subjectSys;
	
	private String addtax;
	
	private String incometax;
	
	private String maker;
	
	private String startYear;
	
	private Boolean invited;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}



	public Integer getYongyouId() {
		return yongyouId;
	}

	public void setYongyouId(Integer yongyouId) {
		this.yongyouId = yongyouId;
	}

	public String getBookName() {
		return bookName;
	}

	public void setBookName(String bookName) {
		this.bookName = bookName;
	}

	public Integer getAccSysId() {
		return accSysId;
	}

	public void setAccSysId(Integer accSysId) {
		this.accSysId = accSysId;
	}

	public String getAccSysName() {
		return accSysName;
	}

	public void setAccSysName(String accSysName) {
		this.accSysName = accSysName;
	}

	public String getStartPeriod() {
		return startPeriod;
	}

	public void setStartPeriod(String startPeriod) {
		this.startPeriod = startPeriod;
	}

	public String getSubjectSys() {
		return subjectSys;
	}

	public void setSubjectSys(String subjectSys) {
		this.subjectSys = subjectSys;
	}

	public String getAddtax() {
		return addtax;
	}

	public void setAddtax(String addtax) {
		this.addtax = addtax;
	}

	public String getIncometax() {
		return incometax;
	}

	public void setIncometax(String incometax) {
		this.incometax = incometax;
	}

	public String getMaker() {
		return maker;
	}

	public void setMaker(String maker) {
		this.maker = maker;
	}

	public String getStartYear() {
		return startYear;
	}

	public void setStartYear(String startYear) {
		this.startYear = startYear;
	}

	public Boolean getInvited() {
		return invited;
	}

	public void setInvited(Boolean invited) {
		this.invited = invited;
	}

	public String getTaxNo() {
		return taxNo;
	}

	public void setTaxNo(String taxNo) {
		this.taxNo = taxNo;
	}
	
	
}
