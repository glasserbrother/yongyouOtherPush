package com.zgcfo.ezg.entity.yongyouenum;

public enum FrequencyType {
	年(1),
	月(2),
	无限制(3),
	不发短信(4),
	不显示PC(5);
	
	private int value;
	
	private FrequencyType(int value){
		this.value = value;
	}
	public int getValue(){
		return this.value;
	}
}
