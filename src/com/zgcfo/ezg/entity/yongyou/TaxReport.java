package com.zgcfo.ezg.entity.yongyou;

import java.io.Serializable;

public class TaxReport implements Serializable{
	private String subjectNo;
	private String subjectName;
	private String _year;
	private Double _1;
	private Double _2;
	private Double _3;
	private Double _4;
	private Double _5;
	private Double _6;
	private Double _7;
	private Double _8;
	private Double _9;
	private Double _10;
	private Double _11;
	private Double _12;
	private String isaccountingNum;
	private String measuringUnit;
	private String subjectId;
	private String yongyouId;
	private String uniqueId;
	private String accountBookId;
	private String period;
	
	
	public String getPeriod() {
		return period;
	}
	public void setPeriod(String period) {
		this.period = period;
	}
	public String getSubjectNo() {
		return subjectNo;
	}
	public void setSubjectNo(String subjectNo) {
		this.subjectNo = subjectNo;
	}
	public String getSubjectName() {
		return subjectName;
	}
	public void setSubjectName(String subjectName) {
		this.subjectName = subjectName;
	}
	public String get_year() {
		return _year;
	}
	public void set_year(String _year) {
		this._year = _year;
	}
	public Double get_1() {
		return _1;
	}
	public void set_1(Double _1) {
		this._1 = _1;
	}
	public Double get_2() {
		return _2;
	}
	public void set_2(Double _2) {
		this._2 = _2;
	}
	public Double get_3() {
		return _3;
	}
	public void set_3(Double _3) {
		this._3 = _3;
	}
	public Double get_4() {
		return _4;
	}
	public void set_4(Double _4) {
		this._4 = _4;
	}
	public Double get_5() {
		return _5;
	}
	public void set_5(Double _5) {
		this._5 = _5;
	}
	public Double get_6() {
		return _6;
	}
	public void set_6(Double _6) {
		this._6 = _6;
	}
	public Double get_7() {
		return _7;
	}
	public void set_7(Double _7) {
		this._7 = _7;
	}
	public Double get_8() {
		return _8;
	}
	public void set_8(Double _8) {
		this._8 = _8;
	}
	public Double get_9() {
		return _9;
	}
	public void set_9(Double _9) {
		this._9 = _9;
	}
	public Double get_10() {
		return _10;
	}
	public void set_10(Double _10) {
		this._10 = _10;
	}
	public Double get_11() {
		return _11;
	}
	public void set_11(Double _11) {
		this._11 = _11;
	}
	public Double get_12() {
		return _12;
	}
	public void set_12(Double _12) {
		this._12 = _12;
	}
	public String getIsaccountingNum() {
		return isaccountingNum;
	}
	public void setIsaccountingNum(String isaccountingNum) {
		this.isaccountingNum = isaccountingNum;
	}
	public String getMeasuringUnit() {
		return measuringUnit;
	}
	public void setMeasuringUnit(String measuringUnit) {
		this.measuringUnit = measuringUnit;
	}

	public String getSubjectId() {
		return subjectId;
	}
	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}
	public String getYongyouId() {
		return yongyouId;
	}
	public void setYongyouId(String yongyouId) {
		this.yongyouId = yongyouId;
	}
	public String getUniqueId() {
		return uniqueId;
	}
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
	public Double getByperiod(String period){
		if(period.endsWith("01")){
			return this.get_1();
		}else if(period.endsWith("02")){
			return this.get_2();
		}else if(period.endsWith("03")){
			return this.get_3();
		}else if(period.endsWith("04")){
			return this.get_4();
		}else if(period.endsWith("05")){
			return this.get_5();
		}else if(period.endsWith("06")){
			return this.get_6();
		}else if(period.endsWith("07")){
			return this.get_7();
		}else if(period.endsWith("08")){
			return this.get_8();
		}else if(period.endsWith("09")){
			return this.get_9();
		}else if(period.endsWith("10")){
			return this.get_10();
		}else if(period.endsWith("11")){
			return this.get_11();
		}else if(period.endsWith("12")){
			return this.get_12();
		}
		return 0d;
		
	}
	public String getAccountBookId() {
		return accountBookId;
	}
	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}
	
	
}
