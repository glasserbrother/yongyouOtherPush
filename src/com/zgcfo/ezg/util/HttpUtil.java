package com.zgcfo.ezg.util;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;


public class HttpUtil {
 
 

 /**
  * 发送http请求
  * */
 public static String sendPost(String url, String param) {
	 return packPost(url,param,null);
 }    
 /**
  * 发送http请求
  * */
 public static String sendPost(String url, String param,Map params) {
	
	     return packPost(url,param,params);
 }  

 public static String sendGet(String url, String param) {
	 return packGet(url,param,null);
 }
 public static String sendGet(String url, String param,Map params) {
	 return packGet(url,param,params);
 }
 public static String packGet(String url, String param,Map params){

	     String result = "";
	     BufferedReader in = null;
	     try {
	         String urlNameString = url + "?" + param;	    
	         URL realUrl = new URL(urlNameString);
	         // 打开和URL之间的连接
	         URLConnection connection = realUrl.openConnection();
	         // 设置通用的请求属性
	         connection.setRequestProperty("accept", "*/*");
	         connection.setRequestProperty("connection", "Keep-Alive");
	         connection.setRequestProperty("user-agent",
	                 "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
	         if(params!=null){	     
	        
	        	 connection.setRequestProperty("Token",  (String)params.get("access_token"));
	 	         connection.setRequestProperty("Device-Searal", "15");
	 	         connection.setRequestProperty("Device-Name","7-433d06bb");
	 	         connection.setRequestProperty("Device-Type","Android");
	         }
	         // 建立实际的连接
	         connection.connect();
	         // 获取所有响应头字段
	       //  connection.get
	         Map<String, List<String>> map = connection.getHeaderFields();
	       
	         // 定义 BufferedReader输入流来读取URL的响应
	         in = new BufferedReader(new InputStreamReader(
	                 connection.getInputStream(),"UTF-8"));
	         String line;
	         while ((line = in.readLine()) != null) {
	             result += line;
	         }
	     } catch (Exception e) {
	         //System.out.println("发送GET请求出现异常！" + e);       
	     }
	     // 使用finally块来关闭输入流
	     finally {
	         try {
	             if (in != null) {
	                 in.close();
	             }
	         } catch (Exception e2) {
	             e2.printStackTrace();
	         }
	     }
     return result;
 
 }
 public static String packPost(String url, String param,Map params){

	 OutputStreamWriter out = null;
     BufferedReader in = null;
     String result = "";
	 try {
	     URL realUrl = new URL(url);	   
	     // 打开和URL之间的连接
	     URLConnection conn = realUrl.openConnection();
	     // 设置通用的请求属性
	     conn.setRequestProperty("accept", "*/*");
	     conn.setRequestProperty("connection", "Keep-Alive");
	     conn.setRequestProperty("user-agent",
	             "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
	     // 发送POST请求必须设置如下两行
	     if(params!=null){	        
	    	 conn.setRequestProperty("Token", (String)params.get("access_token"));
	    	 conn.setRequestProperty("Device-Searal", "15");
	    	 conn.setRequestProperty("Device-Name","7-433d06bb");
	    	 conn.setRequestProperty("Device-Type","Android");
         }
	   
	     conn.setDoOutput(true);
	     conn.setDoInput(true);	     
	     // 获取URLConnection对象对应的输出流
	     out = new OutputStreamWriter(conn.getOutputStream(),"UTF-8");
	     // 发送请求参数
	     out.write(param);
	     // flush输出流的缓冲
	     out.flush();
	     // 定义BufferedReader输入流来读取URL的响应
	     in = new BufferedReader(
	             new InputStreamReader(conn.getInputStream(),"UTF-8"));
	     String line;
	     while ((line = in.readLine()) != null) {
	         result += line;
	     }
	 } catch (Exception e) {
	  //   System.out.println("发送 POST 请求出现异常！"+e);
	    // e.printStackTrace();
	 }
	 //使用finally块来关闭输出流、输入流
	     finally{
	         try{
	             if(out!=null){
	                 out.close();
	             }
	             if(in!=null){
	                 in.close();
	             }
	         }
	         catch(IOException ex){
	             ex.printStackTrace();
	         }
	     }
	     return result;
	
 }
 
 
 public static String packEkuaiBaoPost(String url,String param){
	 return packEKuaiBaoPost(url,param, null);
 }
 
 public static String packEKuaiBaoPost(String url, String param,String token){

	 OutputStreamWriter out = null;
     BufferedReader in = null;
     String result = "";
	 try {
	     URL realUrl = new URL(url);	   
	     // 打开和URL之间的连接
	     URLConnection conn = realUrl.openConnection();
	     // 设置通用的请求属性
	     conn.setRequestProperty("accept", "*/*");
	     conn.setRequestProperty("connection", "Keep-Alive");
	     conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
	     conn.setRequestProperty("user-agent",
	             "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
	     if(token != null) 
	    	  conn.setRequestProperty("Authorization", token);
	     
	    	
	     
	     
	     conn.setDoOutput(true);
	     conn.setDoInput(true);	     
	     // 获取URLConnection对象对应的输出流
	     out = new OutputStreamWriter(conn.getOutputStream(),"UTF-8");
	     // 发送请求参数
	     if(param != null)
	     out.write(param);
	     // flush输出流的缓冲
	     out.flush();
	     // 定义BufferedReader输入流来读取URL的响应
	     in = new BufferedReader(
	             new InputStreamReader(conn.getInputStream(),"UTF-8"));
	     String line;
	     while ((line = in.readLine()) != null) {
	         result += line;
	     }
	 } catch (Exception e) {
	     System.out.println("发送 POST 请求出现异常！"+e);
	     e.printStackTrace();
	 }
	 //使用finally块来关闭输出流、输入流
	     finally{
	         try{
	             if(out!=null){
	                 out.close();
	             }
	             if(in!=null){
	                 in.close();
	             }
	         }
	         catch(IOException ex){
	             ex.printStackTrace();
	         }
	     }
	     return result;
	
 }
 public static String sendPost(String url, Map<String, Object> queryParams) {
	// TODO Auto-generated method stub
	return sendPost(url,getUrlParamsByMap(queryParams));
 }
 /** 
  * 将map转换成url 
  * @param map 
  * @return 
  */  
 public static String getUrlParamsByMap(Map<String, Object> map) {  
     if (map == null) {  
         return "";  
     }  
     StringBuffer sb = new StringBuffer();  
     for (Map.Entry<String, Object> entry : map.entrySet()) {  
         sb.append(entry.getKey() + "=" + entry.getValue());  
         sb.append("&");  
     }  
     String s = sb.toString();  
     if (s.endsWith("&")) {  
         s = org.apache.commons.lang.StringUtils.substringBeforeLast(s, "&");  
     }  
     return s;  
 }  
}
