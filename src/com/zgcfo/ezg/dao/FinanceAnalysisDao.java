package com.zgcfo.ezg.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zgcfo.ezg.util.AppMySQLConn;

public class FinanceAnalysisDao {
	
	public List<Map<String, Object>>  getFinanceAnalysis(String month, int taxType){
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_finance_analysis ";
				
					   sql += " where check_period like ?";
					   sql += " and status = 1";	
					   sql += " and (tax_type = ? or tax_type =3)";
					   ps = conn.prepareStatement(sql);
					   
					   ps.setString(1, "%"+month+"%");
					   ps.setInt(2, taxType);
					 
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{
				if(conn != null){
					try {
						conn.close();
					} catch (SQLException e) {
						
						e.printStackTrace();
					}
				}
				
			}
	
	}
	
	public   List<Map<String,Object>> resultSetToList(ResultSet rs) throws java.sql.SQLException {   
		
        if (rs == null)   
            return Collections.emptyList();  
        
        ResultSetMetaData md = rs.getMetaData(); //得到结果集(rs)的结构信息，比如字段数、字段名等   
        int columnCount = md.getColumnCount(); //返回此 ResultSet 对象中的列数   
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();   
        Map<String,Object> rowData = new HashMap<String,Object> ();   
        while (rs.next()) {   
         rowData = new HashMap<String,Object> (columnCount);   
         for (int i = 1; i <= columnCount; i++) {   
                 rowData.put(md.getColumnName(i), rs.getObject(i));   
         }  
         
         list.add(rowData);   
       
        }   
        
        return list;   
	}

	public int getFinancAnalysisSendCountByPeriod(String accountBookId,
			String analysisId, String period, int i) {
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select count(*) as count from yzg_yy_finance_analysis_log ";
				
					   sql += " where period like ? ";
					
					   sql += " and account_book_id = ? " ;
					   
					   sql += " and finance_analysis_id = ? ";
					   
					   sql +=  " and analysis_result  = ? ";
					 
					   ps = conn.prepareStatement(sql);
					   
					   ps.setString(1, "%"+period+"%");
					   ps.setString(2, accountBookId);
					   ps.setString(3, analysisId);
					   ps.setInt(4,i);
					   
					   
					 
					   rs = ps.executeQuery();
						rs.next();
					
				
				return rs.getInt("count");
			} catch (Exception e) {
				e.printStackTrace();
				return 0;
			} finally{
				if(conn != null){
					try {
						conn.close();
					} catch (SQLException e) {
						
						e.printStackTrace();
					}
				}
				
			}
	
	}

	public void createFinancAnalysisLog(String accountBookId,
			String analysisId, String period, int i) {

		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		
			try {
				
				String sql  = " INSERT INTO yzg_oa.yzg_yy_finance_analysis_log ";
				sql += "(finance_analysis_log_id, account_book_id,finance_analysis_id,  ";
				sql += " analysis_result, create_date,period)";
				sql += "VALUES(uuid(),?, ?,	?,now(),?)"; 
				
				ps = conn.prepareStatement(sql);
				   
			   ps.setString(1, accountBookId);
			   ps.setString(2, analysisId);
			   ps.setInt(3,i);
			   ps.setString(4, period);

			   ps.executeUpdate();
				

			} catch (Exception e) {
				e.printStackTrace();
			
			} finally{
				if(conn != null){
					try {
						conn.close();
					} catch (SQLException e) {
						
						e.printStackTrace();
					}
				}
				
			}
	
	
	}
}
