package com.zgcfo.ezg.entity.yongyou;

/**
 * 
 * 易代账用户对象
 * **/
public class User {
	
	/**
	 * 用户id
	 * **/
	private Integer userId;
	
	/**
	 * 公司id
	 * **/
	private Integer cmpyId;
	
	/**
	 * 虚机域名前缀
	 * **/
	private String cspOrgAccount;
	
	/**
	 * 应用类型
	 * 0: 初始化默认值；4：公司版；5:好会计；6：老板账号；7：易代账个人版工作台模式；8:易代账非工作台模式；9：未开通应用
	 * **/
	private Integer appType;
	
	/**
	 * 角色
	 * 1:记账会计；2：主管；3：管理员；50：助理会计；51：出纳
	 * **/
	private Integer role;
	
	/**
	 * 用户姓名
	 * **/
	private String name;
	
	/***
	 * 用户手机
	 * */
	private String mobile;
	
	/**
	 * 用户邮箱
	 * **/
	private String email;

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public Integer getCmpyId() {
		return cmpyId;
	}

	public void setCmpyId(Integer cmpyId) {
		this.cmpyId = cmpyId;
	}

	public String getCspOrgAccount() {
		return cspOrgAccount;
	}

	public void setCspOrgAccount(String cspOrgAccount) {
		this.cspOrgAccount = cspOrgAccount;
	}

	public Integer getAppType() {
		return appType;
	}

	public void setAppType(Integer appType) {
		this.appType = appType;
	}

	public Integer getRole() {
		return role;
	}

	public void setRole(Integer role) {
		this.role = role;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
	
}
