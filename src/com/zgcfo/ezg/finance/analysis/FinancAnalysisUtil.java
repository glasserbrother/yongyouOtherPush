package com.zgcfo.ezg.finance.analysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.zgcfo.ezg.dao.FinanceAnalysisDao;
import com.zgcfo.ezg.dao.YongYouDataDao;
import com.zgcfo.ezg.entity.yongyouenum.FinanceAanalysisData;

/***
 * 财务数据分析工具类
 * 
 * **/
public class FinancAnalysisUtil{
	
	private YongYouDataDao dataDao;
	
	private FinanceAnalysisDao financeDao;
	
	private List<Map<String,Object>> balanceMaps;
	private List<Map<String,Object>> incomeMaps ;
	private List<Map<String,Object>> incomeQuarterMaps;
	private List<Map<String,Object>> balanceReportMap ;
	
	
	/**
	 * 根据key获取目标对象的值
	 * @param maps 目标对象
	 * @param  key
	 * **/
	public Object getMapValue(List<Map<String,Object>> maps,String key){
		Object obj = null;
		if(maps != null  && !maps.isEmpty()){
			
			Map<String,Object> resultMap = maps.get(0);
			
			obj = resultMap.get(key);
			
		}
		
		return obj;
	}
	
	
	public String getSbjIdByName(String sbjName){
		
		String sbjId ="";
		
		List<Map<String,Object>> sbjMaps =  dataDao.getSubjectByName(accountBookId, sbjName);
		
		if( getMapValue(sbjMaps,"yongyou_id") != null){
			sbjId =    getMapValue(sbjMaps,"yongyou_id")+"";
		}
		
		return sbjId;
		
	}

	
	private String accountBookId;
	
	private String period;
	
	public YongYouDataDao getDataDao() {
		return dataDao;
	}

	public void setDataDao(YongYouDataDao dataDao) {
		this.dataDao = dataDao;
	}

	public FinanceAnalysisDao getFinanceDao() {
		return financeDao;
	}

	public void setFinanceDao(FinanceAnalysisDao financeDao) {
		this.financeDao = financeDao;
	}

	
	public FinancAnalysisUtil(YongYouDataDao yonyouDataDao,FinanceAnalysisDao funanaceAnalysisDao,String accounBookId,String period){
		
		this.setDataDao(yonyouDataDao);
		this.setFinanceDao(funanaceAnalysisDao);
		this.accountBookId = accounBookId;
		this.period = period;
		
	}
	/**
	 * 获取需要的财务指标数据
	 * **/
	public Map<Integer,Object> getData(){
		Map<Integer,Object> dataMap = new HashMap<Integer,Object>();
		
		balanceMaps = dataDao.getBalances(accountBookId, period, period);
		incomeMaps = dataDao.getIncomeReports(accountBookId, period);
		incomeQuarterMaps = dataDao.getIncomeReportQuarters(accountBookId, period);
		balanceReportMap = dataDao.getBalanceReport(accountBookId, period);
		
		FinanceAanalysisData[] fs = FinanceAanalysisData.values();
		for(int i =0;i<fs.length;i++){
			dataMap.put(fs[i].getValue(), getDataByDouble(fs[i]));
		}
		
		//无形资本或股本前一月数据需要特殊处理
		String year  = period.substring(0,4);
		String month = period.substring(4,6);
		String uPeriod = "";
		if(!period.endsWith("01")){
			String monthStr  = (Integer.valueOf(month) -1)+"";
			month = monthStr.length()==1?"0"+monthStr:monthStr;
			uPeriod = year + month;
		}else{
			uPeriod = (Integer.valueOf(year) - 1)+"12";
		}
		double  uwxzc = 0;
		double ussgb = 0;
		
		getBalanceReportByAsset(accountBookId, uPeriod, "无形资产", "asset_end_balance", uwxzc);
		getBalanceReportByEquity(accountBookId, uPeriod, "实收资本（或股本）", "equity_end_balance", ussgb);
		
		dataMap.put(FinanceAanalysisData.无形资产前一月.getValue(), uwxzc);
		dataMap.put(FinanceAanalysisData.实收资本前一月.getValue(), ussgb);
		
		//纳税统计表需要特殊处理
		List<Map<String,Object>> maps =  dataDao.getTaxReport(accountBookId,period);
			
		String yjsf = "";
		String yjsfJd = "";
		String head = ",缴纳";
		for(Map<String,Object> map : maps){
			String yjName = (String) map.get("subject_name");
			if(!"合计".equals(yjName)){
				
				yjName = yjName.replace("应交", "");
				yjsf += yjName +map.get("_"+Integer.valueOf( period.substring(4,6)))+ "元,";
				yjsfJd += yjName +getJdSf(map,Integer.valueOf( period.substring(4,6)))+ "元,";
				
			}
			
		}
		
		if(yjsf.length() >0){
			yjsf = yjsf.substring(0, yjsf.length()-1);
			dataMap.put(FinanceAanalysisData.应缴税费.getValue(),head+ yjsf);
		}else{
			dataMap.put(FinanceAanalysisData.应缴税费.getValue(), yjsf);
		}
		if(yjsfJd.length() >0){
			yjsfJd = yjsfJd.substring(0, yjsfJd.length()-1);
			dataMap.put(FinanceAanalysisData.应缴当季度税费.getValue(),head+ yjsfJd);
		}else{
			dataMap.put(FinanceAanalysisData.应缴当季度税费.getValue(), yjsfJd);
		}
	
		return dataMap;
	}
	
	public double getJdSf(Map<String,Object> map,int month){
		double sf = 0;
		if(month == 1 || month == 2 || month == 3){
			sf += (double) map.get("_1");
			sf += (double) map.get("_2");
			sf += (double) map.get("_3");
			
		}
		if(month == 4 || month == 5 || month == 6){
			sf += (double) map.get("_4");
			sf += (double) map.get("_5");
			sf += (double) map.get("_6");
		}
		if(month == 7 || month == 8 || month == 9){
			sf += (double) map.get("_7");
			sf += (double) map.get("_8");
			sf += (double) map.get("_9");
		}
		if(month == 10 || month == 11 || month == 12){
			sf += (double) map.get("_10");
			sf += (double) map.get("_11");
			sf += (double) map.get("_12");
		}

		return sf;
	}
	public	double getDataByDouble( FinanceAanalysisData fs){
		double result = 0;
			
		List<Map<String, Object>> maps = null;
		
		if("balance".equals(fs.getTargetReport())){
			maps = balanceMaps;
		}else if("balanceReport".equals(fs.getTargetReport())){
			maps = balanceReportMap;
		}else if("income".equals(fs.getTargetReport())){
			maps = incomeMaps;
		}else if("incomeQuarter".equals(fs.getTargetReport())){
			maps = incomeQuarterMaps;
		}
		
		if(maps != null){
			String tagerValue = fs.getTargetValue();
			if(fs.isBySbj()){//是否是根据科目Id来获取的
				//先获取科目ID
				tagerValue = getSbjIdByName(tagerValue);
				
			}
			
			for(Map<String,Object> map : maps){
				if(map.get(fs.getTargetkey()).equals(tagerValue)){					
						Object obj  = map.get(fs.getKey());						
						if(obj != null){
							result = (double) obj;
						}
				}
			}
		}		
		return result;
	}
	
	public static void main(String[] args) {
		YongYouDataDao dataDao = new YongYouDataDao();
		
		FinanceAnalysisDao financeDao = new FinanceAnalysisDao();
		
		FinancAnalysisUtil util = new FinancAnalysisUtil(dataDao, financeDao, "a031e2ab-b2da-11e6-94b7-28e347652f73", "201611");
	
		Map<Integer, Object> dataMap = util.getData();
		System.out.println(JSON.toJSONString(dataMap));
	}
	
	public String getAccountBookId() {
		return accountBookId;
	}
	
	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}
	
	public String getPeriod() {
		return period;
	}
	
	public void setPeriod(String period) {
		this.period = period;
	}
	
	/**
	 * 根据科目名称获取余额表的值
	 * @param accountBookId 账簿id
	 * @param period 期间
	 * @param sbjName 科目名称
	 * @param fieldName 字段名称
	 * @param  obj  需要赋值的目标
	 * */
	public void getBalanceBySbjName(String accountBookId,String period,String sbjName,String fieldName,Object obj){
		
		String sbjId ="";
		
		List<Map<String,Object>> sbjMaps =  dataDao.getSubjectByName(accountBookId, sbjName);
		
		if( getMapValue(sbjMaps,"yongyou_id") != null){
			sbjId =   (String) getMapValue(sbjMaps,"yongyou_id");
		}
		
		List<Map<String,Object>> flBalanceMaps = dataDao.getBalanceBySbjId(accountBookId, period, period, sbjId);
	
		if(getMapValue(flBalanceMaps,fieldName) != null){
			obj = getMapValue(flBalanceMaps,fieldName);
		}
	}
	
	/**
	 * 根据资产项名称获取资产负债表的值
	 * @param accountBookId 账簿id
	 * @param period 期间
	 * @param asset 资产项名称
	 * @param fieldName 字段名称
	 * @param  obj  需要赋值的目标
	 * */
	public void getBalanceReportByAsset(String accountBookId,String period,String asset,String fieldName,Object obj){
		List<Map<String,Object>> resultMaps = dataDao.getBalanceReportByAsset(accountBookId, period, asset);

		if(getMapValue(resultMaps,fieldName) != null){
			
			obj = getMapValue(resultMaps,fieldName);
		}
		
	}
	
	/**
	 * 根据负债项名称获取资产负债表的值
	 * @param accountBookId 账簿id
	 * @param period 期间
	 * @param equity 负债项名称
	 * @param fieldName 字段名称
	 * @param  obj  需要赋值的目标
	 * */
	public void getBalanceReportByEquity(String accountBookId,String period,String equity,String fieldName,Object obj){
		List<Map<String,Object>> resultMaps = dataDao.getBalanceReportByEquity(accountBookId, period, equity);
		
		if(getMapValue(resultMaps,fieldName) != null){
			
			obj = getMapValue(resultMaps,fieldName);
		}
		
	}
	
	/**
	 * 根据利润表item 获取利润表的值
	 * @param accountBookId 账簿id
	 * @param period 期间
	 * @param item item
	 * @param fieldName 字段名称
	 * @param  obj  需要赋值的目标
	 * */
	public void getIncomeByItem(String accountBookId,String period,String item,String fieldName,Object obj){
			
		
		List<Map<String,Object>> maps = dataDao.getIncomeReportByItem(accountBookId, period,item);
		
		if( getMapValue(maps,fieldName) != null){
			obj =  getMapValue(maps,fieldName);
		}	
	}
	
	/**
	 * 根据利润季报表item 获取利润表的值
	 * @param accountBookId 账簿id
	 * @param period 期间
	 * @param item item
	 * @param fieldName 字段名称
	 * @param  obj  需要赋值的目标
	 * */
	public void getIncomeQuarterByItem(String accountBookId,String period,String item,String fieldName,Object obj){
			
		
		List<Map<String,Object>> maps = dataDao.getIncomeReportQuarterByItem(accountBookId, period,item);
		
		if( getMapValue(maps,fieldName) != null){
			obj =  getMapValue(maps,fieldName);
		}	
	}
	
}
