package com.zgcfo.ezg.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;

public class AppMySQLConn {
	
	private Connection conn = null;
	private ResultSet rs = null;
	private PreparedStatement ps = null;
	private CallableStatement cstmt = null;

	private boolean bClosed = false;
	private boolean bAutoCommit = true;
	
	private static String database_name="yzg_oa";
	private static String url="jdbc:mysql://localhost:3306/yzg_oa?useUnicode=true&characterEncoding=UTF-8";
	private static String password="sipu_root";
	private static String diver_name="com.mysql.jdbc.Driver";
	private static String username="root";
	
	static{
		
		//init();
		try {
            Class.forName(diver_name);
            System.out.println("load success");
        } catch (ClassNotFoundException ex) {
            System.out.println("Error load" + "com.mysql.jdbc.Driver");
        }
		
	}
	
	public static void init(){
		Properties pps = new Properties();
	    try {
	    	String getCurrentPath = getCurrentPath();
	    	String path = getCurrentPath + "dbconfig.properties";
	    	System.out.println("path:"+path);
	    	//java.io.InputStream in = AppMySQLConn.class.getResourceAsStream("dbconfig.properties");  
			pps.load(new FileInputStream(path));
			 Enumeration enum1 = pps.propertyNames();//得到配置文件的名字
			    while(enum1.hasMoreElements()) {
			        String strKey = (String) enum1.nextElement();
			        String strValue = pps.getProperty(strKey);
			        if ("database_name".equals(strKey)){
			        	database_name = strValue;
			        }
			        if ("url".equals(strKey)){
			        	url = strValue;
			        }
			        if ("password".equals(strKey)){
			        	password = strValue;
			        }
			        if ("diver_name".equals(strKey)){
			        	diver_name = strValue;
			        }
			        if ("username".equals(strKey)){
			        	username = strValue;
			        }
			     }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Connection getConnection() {
		try {
			if (conn == null ){
				conn = java.sql.DriverManager.
			 			getConnection(url, username, password);
			}
		 	
		 	System.out.print("conn"+conn);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err.println("MyJDBC(getConnection):" + ex.getMessage());
			conn = null;
		}
		return conn;
	}

	public PreparedStatement prepareStatement(String sql) {
		try {
			ps = getConnection().prepareStatement(sql);
		} catch (Exception ex) {
			System.err.println("MyJDBC(prepareStatement):" + ex.getMessage());
			ps = null;
		}

		return ps;
	}

	public PreparedStatement prepareStatement(StringBuilder sql) {
		return prepareStatement(sql.toString());
	}

	public CallableStatement prepareCall(String sql) {
		try {
			cstmt = getConnection().prepareCall(sql);
		} catch (Exception ex) {
			System.err.println("MyJDBC(prepareCall):" + ex.getMessage());
			cstmt = null;
		}
		return cstmt;
	}

	private void setAutoCommit(boolean flag) {
		try {
			bAutoCommit = flag;
			getConnection().setAutoCommit(flag);
		} catch (SQLException ex) {
			System.err.println("MyJDBC(setAutoCommit):" + ex.getMessage());
		}
	}

	public void beginTransaction() {
		setAutoCommit(false);
	}

	public void commitTransaction() {
		try {
			if (!bAutoCommit) {
				getConnection().commit();
			}
		} catch (SQLException ex) {
			System.err.println("MyJDBC(commitTransaction):" + ex.getMessage());
		}
	}

	public void rollbackTransaction() {
		try {
			if (!bAutoCommit) {
				getConnection().rollback();
			}
		} catch (SQLException ex) {
			System.err.println("MyJDBC(rollbackTransaction):" + ex.getMessage());
		}
	}

	public void close() {
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (cstmt != null) {
				cstmt.close();
				cstmt = null;
			}
			if (ps != null) {
				ps.close();
				ps = null;
			}
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (Exception ex) {
			System.err.println("MyJDBC(close):" + ex.getMessage());
		} finally {
			conn = null;
			bClosed = true;
		}
	}

	protected void finalize() {
		try {
			if (!bClosed) {
				close();
			}
		} catch (Exception ex) {
			System.err.println("MyJDBC(finalize):" + ex.getMessage());
		}
	}

	public static void main(String[] args) {
		AppMySQLConn app = new AppMySQLConn();
		app.getConnection();
	}
	
	public static String getCurrentPath(){  
	       //取得根目录路径  
			Class c = AppMySQLConn.class;
			System.out.println(c.getName());
			URL url = null;
			String path = c.getProtectionDomain().getCodeSource().getLocation().getPath();
			System.out.println("path:"+path);
			url = c.getResource("");			
			System.out.println("url:"+url.toString());
			url = c.getResource("/");
			System.out.println("url:"+url.toString());
			String file = url.getFile();
			System.out.println("file:"+file);
	       String rootPath=AppMySQLConn.class.getResource("/").getFile().toString();  
	       //当前目录路径  
	       //String currentPath1=getClass().getResource(".").getFile().toString();  
	      // String currentPath2=getClass().getResource("").getFile().toString();  
	       //当前目录的上级目录路径  
	      // String parentPath=getClass().getResource("../").getFile().toString();  
	          
	    //   System.out.println("rootPath:"+rootPath);
	    //   System.out.println("currentPath1:"+currentPath1);
	    //   System.out.println("currentPath2:"+currentPath2);
	    //   System.out.println("parentPath:"+parentPath);
	       return rootPath;         
	   
	   }

	public static AppMySQLConn getInstance() {
		// TODO Auto-generated method stub
		return new AppMySQLConn();
	}
	

}
