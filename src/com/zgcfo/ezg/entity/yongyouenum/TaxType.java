package com.zgcfo.ezg.entity.yongyouenum;

public enum TaxType {
	小规模(1,"tax13"),
	一般纳税人(2,"tax17"),
	无限制(3,"");
	
	private int value;
	private String tax;
	private TaxType(int value,String tax){
		this.value = value;
		this.tax = tax;
	}
	public int getValue(){
		return this.value;
	}
	
	public String getTax(){
		
		return tax;
	}
}
