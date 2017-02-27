package com.zgcfo.ezg.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.zgcfo.ezg.entity.yongyou.YongYouDataEntity;
import com.zgcfo.ezg.util.AppMySQLConn;
import com.zgcfo.ezg.util.MyFormat;

public class YongYouDoData {
	
	
	public List<YongYouDataEntity> getYongYouDataList(){
		return getYongYouDataList(null);
	}
	public List<YongYouDataEntity> getYongYouDataList(String period){
		List<YongYouDataEntity> datas = new ArrayList<YongYouDataEntity>();
		AppMySQLConn con = new AppMySQLConn();
		StringBuilder sql ;
		int index = 1;
		
		sql = new StringBuilder();
		sql.append(" delete from yzg_yy_remain where status = '1' ");
		if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
			sql.append(" and period = ? ");
		}
		PreparedStatement psDeleteRemain = con.prepareStatement(sql);
		
		sql = new StringBuilder();
		sql.append(" insert into yzg_yy_remain select id ,tableType ,accountantLoginName ,accountantPwd ,accountBookId ,yongyouBookId ,period ,subjectId ,errMsg ,createDate ,status   From yzg_yy_logerr where status = '1' ");
		if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
			sql.append(" and period = ? ");
		}
		PreparedStatement psInsertRemain = con.prepareStatement(sql);
		
		sql = new StringBuilder();
		sql.append(" delete from yzg_yy_logerr where status = '1'  ");
		if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
			sql.append(" and period = ? ");
		}
		PreparedStatement psDeleteLogerr = con.prepareStatement(sql);
		
		
		
		con.beginTransaction();
		try {
			index = 1;
			if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
				psDeleteRemain.setString(index++, period);
			}
			index = 1;
			if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
				psInsertRemain.setString(index++, period);
			}
			index = 1;
			if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
				psDeleteLogerr.setString(index++, period);
			}
			psDeleteRemain.executeUpdate();
			psInsertRemain.executeUpdate();
			psDeleteLogerr.executeUpdate();
			con.commitTransaction();
		} catch (SQLException e1) {
			e1.printStackTrace();
			con.rollbackTransaction();
			return null;
		}
		
		sql = new StringBuilder();
		sql.append(" select  * From yzg_yy_remain  where status = '1'  ");
		if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
			sql.append(" and period = ? ");
		}
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs;
		try {
			index = 1;
			if (!MyFormat.isStrNull(period) && !"noneed".equals(period)){
				ps.setString(index++, period);
			}
			rs = ps.executeQuery();
			String loginName;
			String pwd;
			String bookId;
			int yongyouId;
			String currMonth;
			String subjectId;
			String tableType; 
			
			YongYouDataEntity yyData;
			while(rs.next()){
				loginName = rs.getString("accountantLoginName");
				pwd = rs.getString("accountantPwd");
				bookId = rs.getString("accountBookId");
				yongyouId = rs.getInt("yongyouBookId");
				currMonth = rs.getString("period");
				subjectId = rs.getString("subjectId");
				tableType = rs.getString("tableType");
				yyData = new YongYouDataEntity();
				yyData.setLoginName(loginName);
				yyData.setPwd(pwd);
				yyData.setBookId(bookId);
				yyData.setYongyouId(yongyouId);
				yyData.setCurrMonth(currMonth);
				yyData.setSubjectId(subjectId);
				yyData.setTableType(tableType);
				
				datas.add(yyData);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return datas;
	}
	
	public static void main(String[] args) {
		YongYouDoData yyDo= new YongYouDoData();
		String period = "201609";
		yyDo.getYongYouDataList(null);
	}
	
}
