package com.zgcfo.ezg.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.zgcfo.ezg.util.AppMySQLConn;


/**
 * 用友dao
 * 
 * **/
public class YongYouDataDao {
	
	
	/**
	 * 获取所有账簿
	 * 
	 * */
	public List<Map<String, Object>>  getAccountBooks(){		
		return getAccountBooks(null);
	}
	
	

	/**
	 * 根据账簿id获取账簿，
	 * @param bookId 账簿id 不传则默认查询所有账簿
	 * 
	 * */
	public List<Map<String, Object>> getAccountBooks(String bookId){
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_accountbook ";
				if( bookId != null){
					sql += " where bookid = ?";
				}
				
				ps = conn.prepareStatement(sql);
				
				if(bookId != null){
					ps.setString(1, bookId);
				}
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
					try {
						if(conn != null){
							
						   conn.close();
						
						}
						if(ps != null){
							
							ps.close();
							ps = null;
						}
						
						if(rs != null){
							
							rs.close();
							rs = null;
						}
					} catch (SQLException e) {
						
						e.printStackTrace();
					}
				
				
			}
	
	}
	
	/**
	 * 根据账簿id获取所有科目
	 * @param accountBookId 必填不可为空
	 * **/
	public List<Map<String, Object>>  getSubjects(String accountBookId){
		return getSubjectById(accountBookId,null);
	}
	
	/***
	 * 根据账簿Id可科目Id获取账簿
	 * @param accountBookId 必传
	 * @param subjectId 可为空 为空则表示查询所有科目
	 * **/
	public List<Map<String, Object>>  getSubjectById(String accountBookId,String subjectId){
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_subject  ";
				
					   sql += " where account_book_id = ?";
					
					   if(subjectId != null){
						   
						   sql += " and yongyou_id = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					   
					   ps.setString(1, accountBookId);
					   
					   if(subjectId != null){
							ps.setString(2, subjectId);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	}
	
	/***
	 * 根据账簿Id和科目名称获取账簿
	 * @param accountBookId 必传
	 * @param subjectId 可为空 为空则表示查询所有科目
	 * **/
	public List<Map<String, Object>>  getSubjectByName(String accountBookId,String subjectName){
		String [] sbjNames = subjectName.split("-");
		
		List<String> namesTemp = Arrays.asList(sbjNames);
		List<String> names = new ArrayList<String>(namesTemp);
		String sbjName = names.get(names.size()-1);
		
		names.remove(names.size()-1);
			
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select sj0.* from yzg_yy_subject  sj0 ";
						
						for(int k = 0;k<names.size();k++){
							sql += " , yzg_yy_subject sj"+(k+1);
						}
				
					   sql += " where sj0.account_book_id = ?";
					
						for(int k = 0;k<names.size();k++){
							sql += " and LEFT(sj"+k+".subject_no,LENGTH(sj"+(k+1)+".subject_no))="+"sj"+(k+1)+".subject_no";
							sql += " and sj"+(k+1)+".subject_text =  ?";
						}
				
					   if(subjectName != null){
						   
						   sql += " and sj0.subject_text = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  int index = 1;
					   ps.setString(index++, accountBookId);
						for(int k = 0;k<names.size();k++){
							   ps.setString(index++, names.get(k));
							
						}
				
					   if(sbjName != null){
							ps.setString(index++, sbjName);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	}
	
	
	
	/**
	 * 获取利润表
	 * @param accountBookId 账簿id 不可空
	 * @param preiod 期间id 不可为空
	 * **/
	public List<Map<String, Object>>  getIncomeReports(String accountBookId,String period){
		return getIncomeReportByItem(accountBookId,period,null);
	}
	
	/***
	 * 获取利润表
	 * @param accountBookId 账簿id 不可为空
	 * @param preiod 期间id 不可为空
	 * @param item 完整项目名称
	 * 
	 * **/
	public List<Map<String, Object>>  getIncomeReportByItem(String accountBookId,String period,String item){
		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_income_report ";
				
					   sql += " where account_book_id = ? and period = ? ";
					
					   if(item != null){
						   
						   sql += " and item = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					   if(item != null){
							ps.setString(3, item);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	
	}
	
	/**
	 * 获取利润季报表
	 * @param accountBookId 账簿id 不可空
	 * @param preiod 期间id 不可为空
	 * **/
	public List<Map<String, Object>>  getIncomeReportQuarters(String accountBookId,String period){
		return getIncomeReportQuarterByItem(accountBookId,period,null);
	}
	
	/***
	 * 获取利润季报表
	 * @param accountBookId 账簿id 不可为空
	 * @param preiod 期间id 不可为空
	 * @param item 完整项目名称
	 * 
	 * **/
	public List<Map<String, Object>>  getIncomeReportQuarterByItem(String accountBookId,String period,String item){
		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_income_quarter_report ";
				
					   sql += " where account_book_id = ? and period = ? ";
					
					   if(item != null){
						   
						   sql += " and item = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					   if(item != null){
							ps.setString(3, item);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	
	}
	
	public List<Map<String, Object>>  getCashReports(String accountBookId,String period){
		return getCashReportByItem(accountBookId,period,null);
	}
	
	public List<Map<String, Object>>  getCashReportByItem(String accountBookId,String period,String item){

		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_cash_report ";
				
					   sql += " where account_book_id = ? and period = ? ";
					
					   if(item != null){
						   
						   sql += " and item = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					   if(item != null){
							ps.setString(3, item);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	}
	
	
	/***
	 * @param accountBookId 账簿ID 不可为空
	 * @param start_period 开始期间 不可为空
	 * @param end_period 结束期间 不可为空
	 * ***/
	public List<Map<String, Object>>  getBalances(String accountBookId,String startPeriod,String endPeriod){
		return getBalanceBySbjName(accountBookId,startPeriod,endPeriod,null);
	}
	
	/***
	 * @param accountBookId 账簿ID 不可为空
	 * @param start_period 开始期间 不可为空
	 * @param end_period 结束期间 不可为空
	 * @param subjectName 科目id
	 * ***/
	public List<Map<String, Object>>  getBalanceBySbjId(String accountBookId,String startPeriod,String endPeriod,String subjectId){
		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_balance ";
				
					   sql += " where account_book_id = ? and start_period = ? and end_period = ? ";
					
					   if(subjectId != null){
						   
						   sql += " and subject_id = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, startPeriod);
					   ps.setString(3, endPeriod);
					   if(subjectId != null){
							ps.setString(4, subjectId);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	
	}
	
	/***
	 * @param accountBookId 账簿ID 不可为空
	 * @param start_period 开始期间 不可为空
	 * @param end_period 结束期间 不可为空
	 * @param subjectName 科目名称
	 * ***/
	public List<Map<String, Object>>  getBalanceBySbjName(String accountBookId,String startPeriod,String endPeriod,String subjectName){
	
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_balance ";
				
					   sql += " where account_book_id = ? and start_period = ? and end_period = ? ";
					
					   if(subjectName != null){
						   
						   sql += " and subject_name = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, startPeriod);
					   ps.setString(3, endPeriod);
					   if(subjectName != null){
							ps.setString(4, subjectName);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	}
	
	public List<Map<String, Object>>  getBalanceReport(String accountBookId,String period){
		return  getBalanceReportByEquity( accountBookId, period,null);
	}
	public List<Map<String, Object>>  getBalanceReportByEquity(String accountBookId,String period,String equity){

		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_balance_report ";
				
					   sql += " where account_book_id = ? and period = ?  ";
					
					   if(equity != null){
						   
						   sql += " and equity = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					
					   if(equity != null){
							ps.setString(3, equity);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	
	}
	public List<Map<String, Object>>  getBalanceReportByAsset(String accountBookId,String period,String asset){

		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_balance_report ";
				
					   sql += " where account_book_id = ? and period = ?  ";
					
					   if(asset != null){
						   
						   sql += " and asset = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					
					   if(asset != null){
							ps.setString(3, asset);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	}
	
	public List<Map<String, Object>>  getDetailReport(String accountBookId,String startPeriod,String endPeriod){
		return getDetailReportBySummary(accountBookId,startPeriod,endPeriod,null);
	}
	
	public List<Map<String, Object>>  getDetailReportBySummary(String accountBookId,String startPeriod,String endPeriod,String summary){
	
		return getDetailReportBySbjIdAndSummary(accountBookId,startPeriod,endPeriod,null,summary);
	
	}
	public List<Map<String, Object>>  getDetailReportBySbjId(String accountBookId,String startPeriod,String endPeriod,String subjectId){
		
		return getDetailReportBySbjIdAndSummary(accountBookId,startPeriod,endPeriod,subjectId,null);
	}
	
	public List<Map<String, Object>>  getDetailReportBySbjIdAndSummary(String accountBookId,String startPeriod,String endPeriod,String subjectId,String summary){


		
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_detail_account_report ";
				
				  sql += " where account_book_id = ? and start_period = ? and end_period = ? ";
					
					
					   if(subjectId != null){
						   
						   sql += " and subject_id = ?";
					   }
				
					   if( summary != null ){
						   
						   sql += " and summary = ?";
						   
					   }
					   ps = conn.prepareStatement(sql);
					   int index = 1;
					   ps.setString(index++, accountBookId);
					   ps.setString(index++, startPeriod);
					   ps.setString(index++, endPeriod);
					
					   if(subjectId != null){
						   
						   ps.setString(index++, subjectId);
						   
					   }
				
					   if( summary != null ){
						   
						   ps.setString(index++, summary);
					   }
					   
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	
	}

	/***
	 * 获取纳税统计表
	 * @param  accountBookId 账簿id 不可为空
	 * @param  period 期间  不可为空
	 * **/
	public List<Map<String,Object>> getTaxReport(String accountBookId,String period){
		return getTaxReportBySbjName(accountBookId,period,null);
	}
	
	/***
	 * 根据科目名称获取纳税统计表
	 * @param  accountBookId 账簿id 不可为空
	 * @param  period 期间  不可为空
	 * @param  sbjName 可为空
	 * **/
	public List<Map<String,Object>> getTaxReportBySbjName(String accountBookId,String period,String sbjName){
		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_tax_report ";
				
					   sql += " where account_book_id = ? and period = ?  ";
					
					   if(sbjName != null){
						   
						   sql += " and subject_name = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					
					   if(sbjName != null){
							ps.setString(3,sbjName);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	}
	
	/***
	 * 根据科目id获取纳税统计表
	 * @param  accountBookId 账簿id 不可为空
	 * @param  period 期间  不可为空
	 * @param  sbjId 可为空
	 * **/
	public List<Map<String,Object>> getTaxReportBySbjId(String accountBookId,String period,String subjectId){

		Connection conn = AppMySQLConn.getInstance().getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
			try {
				String sql = "select * from yzg_yy_tax_report ";
				
					   sql += " where account_book_id = ? and period = ?  ";
					
					   if(subjectId != null){
						   
						   sql += " and subject_id = ?";
					   }
				
					   ps = conn.prepareStatement(sql);
					  
					   ps.setString(1, accountBookId);
					   ps.setString(2, period);
					
					   if(subjectId != null){
							ps.setString(3,subjectId);
					   }
				
				rs = ps.executeQuery();
				
				return resultSetToList(rs);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally{				
				try {
					if(conn != null){
						
					   conn.close();
					
					}
					if(ps != null){
						
						ps.close();
						ps = null;
					}
					
					if(rs != null){
						
						rs.close();
						rs = null;
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			
			
		}
	
	}
	public static void main(String[] args) {
		String accountBookId = "26c621fd-b2d6-11e6-94b7-28e347652f73";
		String sbjName = "库存现金";
		String sbjId = "103902";
		String period = "201610";
		
		YongYouDataDao data = new YongYouDataDao();
		
		
		List<Map<String, Object>>  obj = data.getSubjectByName(accountBookId, "短期投资-股票");
		System.out.println(JSON.toJSONString(obj));
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
}
