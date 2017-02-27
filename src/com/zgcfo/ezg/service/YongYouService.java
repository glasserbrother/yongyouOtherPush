package com.zgcfo.ezg.service;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zgcfo.ezg.dao.YongYouDataDao;
import com.zgcfo.ezg.entity.yongyou.Balance;
import com.zgcfo.ezg.entity.yongyou.BalanceReport;
import com.zgcfo.ezg.entity.yongyou.Books;
import com.zgcfo.ezg.entity.yongyou.CashReport;
import com.zgcfo.ezg.entity.yongyou.DetailAccountReport;
import com.zgcfo.ezg.entity.yongyou.IncomeQuarterReport;
import com.zgcfo.ezg.entity.yongyou.IncomeReport;
import com.zgcfo.ezg.entity.yongyou.Subject;
import com.zgcfo.ezg.entity.yongyou.TaxReport;
import com.zgcfo.ezg.util.HttpClientUtil;
import com.zgcfo.ezg.util.LogUtil;
import com.zgcfo.ezg.util.MD5;
import com.zgcfo.ezg.util.MyFormat;
import com.zgcfo.ezg.util.SetUtils;

/***
 * 对用友接口进行封装
 * **/
public class YongYouService {
	
	public final static String SERVICEURL =  "https://ee.chanjet.com";
	public final static String SERVICESTATUSURL = "/api/v1/app/type";
	public final static String GETCIATOKENURL = SERVICEURL + "/auth/login";
	public final static String REQUESTURL = "https://ee.chanjet.com";
	public final static String DEFAULTURL  = REQUESTURL + "/api/v1/ee/appVm";
	public final static String getAllAccount = REQUESTURL +"/api/v1/account/page";
	
	public final static String GETBOOKURL = "/services/1.0/appext/Finance/getBooks";
	public final static String GETINCOMEREPORTURL =  "/services/1.0/appext/IncomeReport/query";
	
	private String ciaToken ;
	
	
	
	public void setCiaToken(String ciaToken) {
		this.ciaToken = ciaToken;
	}
	public boolean initService(String loginName,String password)  throws Exception{
		boolean flag = false;
		Map<String, Object> token =  this.getCiaToken(loginName,MD5.getInstance().getMD5( password).toLowerCase());
		if(token != null &&  MyFormat.formatInt(token.get("code")) == 200){
			flag = true;
			JSONObject obj = (JSONObject) token.get("data");	
			this.setCiaToken( (String) obj.get("ciaToken"));
		}
		return flag;
		
	}
	/**
	 * 根据用户名和密码获取用户访问令牌
	 * @param loginName 用户名
	 * @param password 密码（需要MD5加密）
	 * @return 返回token
	 * @throws UnsupportedEncodingException 
	 * **/
	public Map<String,Object> getCiaToken(String loginName,String password)  throws Exception{
		Map<String,Object> resultMap = null;
		try{
			resultMap = new HashMap<String, Object>();
			Map<String, Object> paramsMap = new HashMap<String, Object>();
			paramsMap.put("loginName", loginName);
			paramsMap.put("password", password);
			String result = HttpClientUtil.httpPostRequest(GETCIATOKENURL,
					paramsMap);
			// System.out.println(result);
			JSONObject jsonObject = (JSONObject) JSON.parse(result);
			Boolean isSuccess = (Boolean) jsonObject.get("success");
			if (isSuccess) {
				resultMap.put("code", 200);
				resultMap.put("msg", "成功");
				resultMap.put("data", jsonObject.get("data"));
				// 调用一遍查询接口状态接口
				JSONObject obj = (JSONObject) resultMap.get("data");
				String token = (String) obj.get("ciaToken");

				paramsMap.put("ciaToken", token);
				Integer state = 0;
				int index = 0;
				while (state != 1) {
					index++;
					if (index > 10) {
						state = 1;
					}
					String resultState = HttpClientUtil.httpGetRequest(
							SERVICEURL + "/api/v1/app/state", paramsMap);
					JSONObject jsonObjectState = (JSONObject) JSON
							.parse(resultState);

					JSONObject jsonData = jsonObjectState.getJSONObject("data");
					state = jsonData.getInteger("state");

				}
				if (index > 10) {
					resultMap.put("code", 500);
					resultMap.put("msg", "接口状态无法正确开启");
				}

			} else {
				resultMap.put("code", 500);
				try {
					resultMap.put("msg", URLDecoder.decode(
							(String) jsonObject.get("message"), "utf-8"));
				} catch (UnsupportedEncodingException e) {
					resultMap.put("msg", jsonObject.get("message"));
				}
			}
		}catch(Exception e){
			//e.printStackTrace();
			throw e;
		}
		
		
		return resultMap;
	}
	
	
	public Map<String,Object> getDataByPost(String url,Class entityClass)  throws Exception{
		return getDataByPost(null,url,entityClass);
	}
	
	public Map<String,Object> getDataByPost(Map<String,Object> paramsMap,String url,Class entityClass)  throws Exception{
		return getData(paramsMap,url,entityClass,"post");
	}
	
	public Map<String,Object> getDataByGet(String url,Class entityClass) throws Exception{
		Map<String,Object> map = null;
		map = getDataByGet(null,url,entityClass);
		return map;
	}
	
	public Map<String,Object> getDataByGet(Map<String,Object> paramsMap,String url,Class entityClass) throws Exception{
		Map<String,Object> map = null;
		map = getData(paramsMap,url,entityClass,"get");
		return map;
	}
	
	public Map<String,Object> getData(Map<String,Object> paramsMap,String url,Class entityClass,String method)  throws Exception{
		Map<String,Object> resultMap = null;
		try{
			resultMap = new HashMap<String,Object>();
			String result = "";
			if("get".equals(method)){
				if(paramsMap != null){
					result = HttpClientUtil.httpGetRequest(url, paramsMap);
				}else{
					result = HttpClientUtil.httpGetRequest(url);
				}
			
			}else{
				if(paramsMap != null){
					result = HttpClientUtil.httpPostRequest(url, paramsMap);
				}else{
					result = HttpClientUtil.httpPostRequest(url);
				}
				
			}
			
			//System.out.println(result);
			JSONObject jsonObject = (JSONObject) JSON.parse(result);
			Boolean isSuccess =  (Boolean) jsonObject.get("success");
			if(isSuccess){
				resultMap.put("code", 200);
				resultMap.put("msg", "成功");
				//sysou
				JSONArray json;
				if(jsonObject.get("data") instanceof JSONObject){
					JSONObject obj = (JSONObject) jsonObject.get("data");
					json = (JSONArray) obj.get("items");
				}else{
					json = (JSONArray) jsonObject.get("data");
				}
					
			
		
				List<Object> objs = new ArrayList<Object>();
				
				for(Object obj :json){
					Map<String,Object> replaceFileds = new HashMap<String,Object>();
					replaceFileds.put("setId", "setYongyouId");
					Object objEn = null;
					try {
						objEn = entityClass.newInstance();
					} catch (Exception e1) {
						//e1.printStackTrace();
						throw e1;
					} 
					try {
						SetUtils.getInstance().setValues((JSONObject)obj, objEn,replaceFileds);
					} catch (Exception e) {				
						//e.printStackTrace();
						throw e;
					} 
					objs.add(objEn);
				}	
				resultMap.put("data", objs);
			}else{
				resultMap.put("code", 500);
				try {
					resultMap.put("msg", URLDecoder.decode(jsonObject.get("message")+"","utf-8"));
				} catch (UnsupportedEncodingException e) {
					resultMap.put("msg",jsonObject.get("message"));
				}
			}
			
		}catch(Exception e){
			//e.printStackTrace();
			throw e;
			//LogUtil.logErr(entityClass.getName(), yongyouBookId, period, e.toString());
			//System.out.println(e.toString());
		}
		
		
		
		return resultMap;
	}
	/**
	 * 获取所有的账簿列表
	 * @return  返回账簿数据
	 * 
	 * **/
	public Map<String,Object> getAllBooks()  throws Exception{	
		Map<String,Object> paramsMap = new HashMap<String,Object>();
		paramsMap.put("ciaToken", ciaToken);
		paramsMap.put("url", "/services/1.0/appext/Finance/getBooks");
		return getDataByGet(paramsMap, DEFAULTURL, Books.class);
	}
	
	public Map<String,Object> getIncomeReport2(Integer bookId,String period)  throws Exception{
		String param = "";
		try {
			param = "ciaToken="+ciaToken+"&url="+"/services/1.0/appext/IncomeReport/query"+"&ACCOUNTBOOK="+bookId+"&queryCond="+URLEncoder.encode("{\"period\": \""+period+"\"}","UTF-8");
		
		} catch (UnsupportedEncodingException e) {
			throw e;
		}	
		String url = DEFAULTURL+"?"+param;
		return getDataByGet(url, IncomeReport.class);
	}
	
	public Map<String,Object> getBlanaceReport(Integer bookId,String period)  throws Exception{
		String param = "";
		try {
			param = "ciaToken="+ciaToken+"&url="+"/services/1.0/appext/BalanceSheet/query"+"&ACCOUNTBOOK="+bookId+""+"&queryCond="+URLEncoder.encode("{\"period\": \""+period+"\"}","UTF-8");
		
		} catch (UnsupportedEncodingException e) {
			throw e;
		}
		
		String url = DEFAULTURL+"?"+param;
		
		return getDataByGet(url, BalanceReport.class);
	}
	

	
	public Map<String, Object> getTaxReport(int bookId, String period) throws Exception {
		

		String param = "";
		try {
			param = "ciaToken="+ciaToken+"&url="+"/services/1.0/appext/TaxStatistics/query"+"&ACCOUNTBOOK="+bookId+""+"&queryCond="+URLEncoder.encode("{\"period\": \""+period+"\"}","UTF-8");
		
		} catch (UnsupportedEncodingException e) {
			throw e;
		}
		
		String url = DEFAULTURL+"?"+param;
	
		return getDataByGet(url, TaxReport.class);
	
	}
	public List<Object> getTaxReports(int bookId, String period)  throws Exception{
		

		String param = "";
		try {
			param = "ciaToken="+ciaToken+"&url="+"/services/1.0/appext/TaxStatistics/query"+"&ACCOUNTBOOK="+bookId+""+"&queryCond="+URLEncoder.encode("{\"period\": \""+period+"\"}","UTF-8");
			String url = DEFAULTURL+"?"+param;
			Map<String,Object> resultMap =  getDataByGet(url, TaxReport.class);
			if(resultMap != null && MyFormat.formatInt(resultMap.get("code")+"") == 200)
				return  (List<Object>) resultMap.get("data");
		} catch (Exception e) {
			throw e;
		} 
		
		
			

		return null;
	
	}
	public List<Object> getReport(String param, Class class1) throws Exception{
		String url = SERVICEURL+"/api/v1/eeIfs/queryReport"+"?"+param;
		Map<String,Object> resultMap = getDataByGet(url, class1);
		if(resultMap != null && MyFormat.formatInt(resultMap.get("code")+"") == 200)
			return  (List<Object>) resultMap.get("data");
			
 		return null;
	}
	
	/***
	 * 
	 * 获取报表
	 * 
	 * */
	public List<Object> getReport(String param,Class clazz,String url)  throws Exception{
		
		Map<String,Object> resultMap = getDataByGet(url+"?"+param, clazz);
		if(resultMap != null && MyFormat.formatInt((resultMap.get("code"))) == 200)
			return  (List<Object>) resultMap.get("data");
			
 		return null;
	}
	/**
	 * 查询资产负债
	 * @param bookId 用友账簿Id
	 * @param period 查询期间 例：201601
	 * @return 返回List<Object> Object 可直接强制转换为BalanceReport 如返回null则接口异常
 	 * **/
	public List<Object> getBalanceReport(Integer bookId,String period)  throws Exception{
		

		 String	param = "ciaToken="+ciaToken+"&bookId="+bookId+"&reportType=debt&period="+period;
		 return getReport(param,BalanceReport.class);
	}
	
	/**
	 * 查询现金流量
	 * @param bookId 用友账簿Id
	 * @param period 查询期间 例：201601
	 * @return 返回List<Object> Object 可直接强制转换为CashReport 如返回null则接口异常
 	 * **/
	public List<Object> getCashReport(Integer bookId,String period) throws Exception{

		 String	param = "ciaToken="+ciaToken+"&bookId="+bookId+"&reportType=cash&period="+period;
		 return getReport(param,CashReport.class);
	}
	
	/***
	 * 获取余额表(余额表)
	 * @param bookId 账套Id
	 * @param startPeriod 开始期间
	 * @param endPeriod 结束期间
	 * */
	public List<Object> getBalance(Integer bookId,String startPeriod,String endPeriod)  throws Exception{
		return getBalance(bookId,startPeriod,endPeriod,0);
	} 
	
	/***
	 * 获取余额表
	 * @param bookId 账套Id
	 * @param startPeriod 开始期间
	 * @param endPeriod 结束期间
	 * @param balanceType 0:余额表 1:数量余额表 2:外币余额表 
	 * 
	 * */
	public List<Object> getBalance(Integer bookId,String startPeriod,String endPeriod,Integer balanceType)  throws Exception{
		String url ="/services/1.0/appext/Balance/query";
		
		String queryCond = "{\"yearAcc\":\"show\",\"period\":\""+startPeriod+"-"+endPeriod+"\",\"level\":\"all\",\"type\":"+balanceType+"}";
		String param = "";
		try {
				param = "ciaToken="+ciaToken+"&ACCOUNTBOOK="+bookId+"&url="+url+"&queryCond="+URLEncoder.encode(queryCond,"UTF-8");

		} catch (Exception e) {
			throw e;
		}
			return getReport(param, Balance.class,DEFAULTURL);
	} 
	
	/**
	 * 查询利润表
	 * @param bookId 用友账簿Id
	 * @param period 查询期间 例：201601
	 * @return 返回List<Object> Object 可直接强制转换为IncomeReport 如返回null则接口异常
 	 * **/
	public List<Object> getIncomeReport(Integer bookId,String period)  throws Exception{

		 String	param = "ciaToken="+ciaToken+"&bookId="+bookId+"&reportType=income&period="+period;
		 return getReport(param,IncomeReport.class);
	}
	
	
	public List<Object> getIncomeQuarterReport(Integer bookId,String period)  throws Exception{
		
		String queryCond = "{\"period\":\""+period+"\"}";
		String param;
		String url = "/services/1.0/appext/IncomeQuarterReport/query";
		try {
				param = "ciaToken="+ciaToken+"&url="+url+"&ACCOUNTBOOK="+bookId+"&queryCond="+URLEncoder.encode(queryCond,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw e;
		}
		
		return getReport(param,IncomeQuarterReport.class,DEFAULTURL);
		
		
		 
	}
	public List<Object> getSubjects(Integer bookId)  throws Exception{
		String url = "/services/1.0/appext/Balance/getBalanceInitList";
		String param =  "ciaToken="+ciaToken+"&bookId="+bookId+"&url="+url;	
		return getReport(param,Subject.class,DEFAULTURL);
	}
	
	public List<Object> getDetailAccountReport(Integer bookId,String startPeriod,String endPeriod,String subjectId)  throws Exception{
		String url = "/services/1.0/appext/DetailAccount/query";
		
		String queryCond = "{\"subject\":\""+subjectId+"\",\"period\":\""+startPeriod+"-"+endPeriod+"\",\"tag\":\"0\",\"isentry\":"+false+"}";
		String param = "";
		try {
				param = "ciaToken="+ciaToken+"&ACCOUNTBOOK="+bookId+"&url="+url+"&queryCond="+URLEncoder.encode(queryCond,"UTF-8");

		} catch (Exception e) {
			throw e;
		}
		return getReport(param,DetailAccountReport.class,DEFAULTURL);
	}
	
	
	/***
	 * 获取首页指标通过本地数据库
	 * @param accountBookId  账簿Id
	 * @param period 期间
	 * 
	 * **/
	public Map<String,Object> getKeyIndex(String accountBookId,String period)  throws Exception{
		
		Map<String,Object> keyIndexMap = null;
		try{
			keyIndexMap = new HashMap<String,Object>();
			double yysr = 0;
			double lrze = 0;
			double cbfy = 0;
			double sj = 0;
			double hbzj = 0;
			YongYouDataDao dao = new YongYouDataDao();
			
			List<Map<String,Object>> taxLists = dao.getTaxReportBySbjName(accountBookId, period, "合计");
			if(taxLists != null){
				
				Map<String,Object> taxMap = taxLists.isEmpty()?null:taxLists.get(0);
				if(taxMap != null){
					sj = MyFormat.formatDouble((Double) taxMap.get("_"+period.substring(3, 5)));
				}
				
				
			}
			
			List<Map<String,Object>> balanceReportLists = dao.getBalanceReportByAsset(accountBookId, period, "货币资金");
			
			if(balanceReportLists != null){
				
				Map<String,Object> balanceReportMap = balanceReportLists.isEmpty()?null:balanceReportLists.get(0);
				hbzj = MyFormat.formatDouble((Double) balanceReportMap.get("asset_end_balance"));
				
			}
			
			List<Map<String,Object>> incomeReports = dao.getBalanceReportByAsset(accountBookId, period, "货币资金");
			
			for(Map<String,Object> incomeReport : incomeReports){
				if("一、营业收入".equals(MyFormat.formatStr(incomeReport.get("item")))){
					yysr = (double) incomeReport.get("month_cash");
				}
				if("三、利润总额（亏损总额以“-”号填列）".equals(MyFormat.formatStr(incomeReport.get("item")))){
					yysr = (double) incomeReport.get("month_cash");
				}
				
				if("减：营业成本".equals(MyFormat.formatStr(incomeReport.get("item")))||"销售费用".equals(MyFormat.formatStr(incomeReport.get("item")))||"管理费用".equals(MyFormat.formatStr(incomeReport.get("item")))||"财务费用".equals(MyFormat.formatStr(incomeReport.get("item")))){
					cbfy += (double) incomeReport.get("month_cash");
				}
			}
			
			keyIndexMap.put("yysr", yysr);
			keyIndexMap.put("lrze", lrze);
			keyIndexMap.put("cbfy", cbfy);
			keyIndexMap.put("sj", sj);
			keyIndexMap.put("hbzj", hbzj);
		}catch(Exception e){
			throw e;
		}
		return keyIndexMap;
	}
	
	/***
	 * 获取首页指标通过接口
	 * @param bookId  用友bookId
	 * @param period 期间
	 * 
	 * **/
	public Map<String,Object> getKeyIndex(Integer bookId, String period)  throws Exception{
		Map<String,Object> keyIndexMap = null;	
		try{
			keyIndexMap = new HashMap<String,Object>();	
			List<Object> incomeReports =  this.getIncomeReport(bookId, period);	
			//System.out.println("income:" + JSON.toJSONString(incomeMap));
			Map<String,Object>  balancReportMap = this.getBlanaceReport(bookId, period);
			//System.out.println("balance:" + JSON.toJSONString(balancReportMap));
			Map<String,Object>  taxReportMap = this.getTaxReport(bookId,period);
			//System.out.println("tax:" + JSON.toJSONString(taxReportMap));
			//List<IncomeReport> incomeReports = incomeMap;
			List<TaxReport> taxReports = (List<TaxReport>) taxReportMap.get("data");
			List<BalanceReport> balanceReports =  (List<BalanceReport>) balancReportMap.get("data");
			double yysr = 0;
			double lrze = 0;
			double cbfy = 0;
			double sj = 0;
			double hbzj = 0;
			IncomeReport incomeReport;
			for(Object o :incomeReports){
				incomeReport = (IncomeReport)o;
				if("一、营业收入".equals(incomeReport.getItem())){
					yysr = incomeReport.getMonthCash();
				}
				if("三、利润总额（亏损总额以“-”号填列）".equals(incomeReport.getItem())){
					lrze = incomeReport.getMonthCash();
				}
				
				if("减：营业成本".equals(incomeReport.getItem())||"销售费用".equals(incomeReport.getItem())||"管理费用".equals(incomeReport.getItem())||"财务费用".equals(incomeReport.getItem())){
					cbfy += incomeReport.getMonthCash();
				}
				
				
				
			}
			for(TaxReport taxReport :taxReports){
				if("合计".equals(taxReport.getSubjectName())){
					sj = taxReport.getByperiod(period);
				}

			}
			for(BalanceReport balanceReport : balanceReports){ 
				if("货币资金".equals(balanceReport.getAsset())){
					hbzj = balanceReport.getAssetEndBalance();
				}
			}

			keyIndexMap.put("yysr", yysr);
			keyIndexMap.put("lrze", lrze);
			keyIndexMap.put("cbfy", cbfy);
			keyIndexMap.put("sj", sj);
			keyIndexMap.put("hbzj", hbzj);
		}catch(Exception e){
			throw e;
		}
		
		return keyIndexMap;
	}
	
	
	public static void main(String[] args) throws Exception, URISyntaxException, NumberFormatException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		//loginName=18675849275, pwd=123456, bookId=a11861d4-b2da-11e6-94b7-28e347652f73, yongyouId=100001, currMonth=201611
		
			//	18675849275	123456	a11861d4-b2da-11e6-94b7-28e347652f73	100001
		YongYouService yongYou = new YongYouService();
		//15920417898	ezg147258	100014
		yongYou.initService("15920417898", "ezg147258");
		yongYou.getAllBooks();
		List<Object> map = yongYou.getSubjects(100014);
		System.out.println(JSON.toJSONString(map));
		List<Object> obj = yongYou.getIncomeReport(100014, "201611");
		System.out.println(JSON.toJSONString(obj));
		List<Object> obj2 = yongYou.getBalanceReport(100014, "201611");
		System.out.println(JSON.toJSONString(obj2));
		List<Object> obj3 = yongYou.getCashReport(100014, "201611");
		System.out.println(obj3.size());
		System.out.println(JSON.toJSONString(obj3));
		List<Object> map2 = yongYou.getIncomeQuarterReport(100014, "201611");
		System.out.println(JSON.toJSONString(map2));
		//System.out.println(JSON.toJSONString(yongYou.getKeyIndex("26c621fd-b2d6-11e6-94b7-28e347652f73", "201610")));
	}
}
