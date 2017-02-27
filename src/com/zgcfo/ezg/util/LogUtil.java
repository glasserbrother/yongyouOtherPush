package com.zgcfo.ezg.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LogUtil {
	
	private static AppMySQLConn conn;
	private static PreparedStatement ps;
	private static PreparedStatement psDetail;
	
	static {
		init();
	}
	
	
	public static void init(){
		System.out.println("init LogUtil.......");
		conn = new AppMySQLConn();
		conn.getConnection();
		
		StringBuilder sql = new StringBuilder();
		sql.append(" insert into yzg_yy_logerr(tableType,accountantLoginName,accountantPwd,accountBookId,yongyouBookId,period,errMsg) values(?,?,?,?,?,?,?) ");
		ps = conn.prepareStatement(sql);
		
		sql = new StringBuilder();
		sql.append(" insert into yzg_yy_logerr(tableType,accountantLoginName,accountantPwd,accountBookId,yongyouBookId,period,subjectId,errMsg) values(?,?,?,?,?,?,?,?) ");
		psDetail = conn.prepareStatement(sql);
	}
	
	public static void logErr(String tableType, String accountantLoginName, String accountantPwd, String accountBookId, int yongyouBookId, String period, String errMsg){
		try {
			ps.setString(1, tableType);
			ps.setString(2, accountantLoginName);
			ps.setString(3, accountantPwd);
			ps.setString(4, accountBookId);
			ps.setInt(5, yongyouBookId);
			ps.setString(6, period);
			ps.setString(7, errMsg);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void logErr(String tableType, String accountantLoginName, String accountantPwd, String accountBookId, int yongyouBookId, String period, String subjectId, String errMsg){
		try {
			psDetail.setString(1, tableType);
			psDetail.setString(2, accountantLoginName);
			psDetail.setString(3, accountantPwd);
			psDetail.setString(4, accountBookId);
			psDetail.setInt(5, yongyouBookId);
			psDetail.setString(6, period);
			psDetail.setString(7, subjectId);
			psDetail.setString(8, errMsg);
			psDetail.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		//LogUtil.logErr("CashReport", 100015, "201611", "balabala");
	}
	
	

}
