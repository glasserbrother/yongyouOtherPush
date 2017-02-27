package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class YongYouDataEntity implements Serializable{
	
	private String loginName;
	private String pwd;
	private String bookId;
	private int yongyouId;
	private String currMonth;
	private String tableType;
	private String subjectId;
	public String getLoginName() {
		return loginName;
	}
	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}	
	public String getCurrMonth() {
		return currMonth;
	}
	public void setCurrMonth(String currMonth) {
		this.currMonth = currMonth;
	}
	public String getBookId() {
		return bookId;
	}
	public void setBookId(String bookId) {
		this.bookId = bookId;
	}
	public int getYongyouId() {
		return yongyouId;
	}
	public void setYongyouId(int yongyouId) {
		this.yongyouId = yongyouId;
	}
	public String getTableType() {
		return tableType;
	}
	public void setTableType(String tableType) {
		this.tableType = tableType;
	}
	public String getSubjectId() {
		return subjectId;
	}
	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}
	@Override
	public String toString() {
		return "YongYouDataEntity [loginName=" + loginName + ", pwd=" + pwd
				+ ", bookId=" + bookId + ", yongyouId=" + yongyouId
				+ ", currMonth=" + currMonth + ", tableType=" + tableType
				+ ", subjectId=" + subjectId + "]";
	}
	
	
	
}
