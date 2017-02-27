package com.zgcfo.ezg.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zgcfo.ezg.dao.FinanceAnalysisDao;
import com.zgcfo.ezg.dao.YongYouDataDao;
import com.zgcfo.ezg.entity.yongyouenum.FinanceAanalysisData;
import com.zgcfo.ezg.entity.yongyouenum.FrequencyType;
import com.zgcfo.ezg.entity.yongyouenum.TaxType;
import com.zgcfo.ezg.finance.analysis.FinancAnalysisUtil;
import com.zgcfo.ezg.util.SetUtils;

public class FinanceAnalysisService{
	
	private String period;
	
	private String companyName;
	
	private Map<Integer,Object> dataMap;

	/**
	 * 检查实收资本是否=0
	 * 
	 * **/
	public Map<String,Object>  check01(Map<String,Object> map){//资产负债表-实收资本（或股本）-期末余额=0 (负债方)

		boolean flag = false;
		String msg = (String) map.get("problem_description");
		msg = msg.replace(":companyName", companyName);
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
				
		if((double)dataMap.get(FinanceAanalysisData.实收资本当月.getValue()) == 0){
			
			flag = true;
		}
	
		resultMap.put("msg", msg);
		resultMap.putAll(map);
		resultMap.put("flag", flag);
		
		return resultMap;
	}
	
	
	public Map<String,Object>   check02(Map<String,Object> map){//取数woa

		Map<String,Object> resultMap = new HashMap<String,Object>();
				
		return resultMap;
	}
	
	public Map<String,Object>  check03(Map<String,Object> map){//余额表-库存现金-期末余额-借方>100000
		//东家您好！您的企业广州***公司现金科目余额超过10万元，避免涉税风险及审计风险，请及时调整。如有疑问请拨打e账柜财税共享服务平台电话400-88-12580（一按我帮您）
		 boolean flag = false;
		
		 String msg = (String) map.get("problem_description");
		 		msg = msg.replace(":companyName", companyName);

		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		 if((double)dataMap.get(FinanceAanalysisData.库存现金.getValue()) > 100000){
				flag = true;			
		 }
		 
		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	}
	

	public Map<String,Object>  check04(Map<String,Object> map){//余额表-管理费用-业务招待费-本年累计借方发生额>利润表-营业收入-本年累计金额*4‰8.33%0 最好
		boolean flag = false;
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		 String msg = (String) map.get("problem_description");
		 		msg = msg.replace(":companyName", companyName);
		 		
		 double ywzdf  = (double) dataMap.get(FinanceAanalysisData.管理费用下业务招待费.getValue());
	
		 double	yysr = (double) dataMap.get(FinanceAanalysisData.利润表下年营业收入.getValue());
		
		if(ywzdf > yysr*0.004){
			flag = true;
			if(ywzdf > yysr*0.008){
				flag = false;
			}
	
		}
		
		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	
	}
	public Map<String,Object>  check05(Map<String,Object> map){//余额表-管理费用-业务招待费-本年累计借方发生额>利润表-营业收入-本年累计金额*4‰8.33%0 最好
		boolean flag = false;
	
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		 String msg = (String) map.get("problem_description");
		 		msg = msg.replace(":companyName", companyName);
		 		
		double ywzdf  = (double) dataMap.get(FinanceAanalysisData.管理费用下业务招待费.getValue());
	
		double yysr = (double) dataMap.get(FinanceAanalysisData.利润表下年营业收入.getValue());
	
		
			if(ywzdf > yysr*0.008){
				flag = true;
			}
	
		
		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	
	}
	
	public Map<String,Object> check06(Map<String,Object> map){		
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;

		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
 		
		//福利费
		double	fl = (double) dataMap.get(FinanceAanalysisData.管理费用下福利费.getValue());
		//研究福利费
		double	yjfl = (double) dataMap.get(FinanceAanalysisData.管理费用下研究费用下福利费.getValue());
		
	
		
		double yfzgxc = (double) dataMap.get(FinanceAanalysisData.管理费用下研究费用下研发人员职工薪酬.getValue());;//研发职工薪酬
		double xszgxc = (double) dataMap.get(FinanceAanalysisData.销售费用下销售人员职工薪酬.getValue());;//销售职工薪酬
		double zgxc =  (double) dataMap.get(FinanceAanalysisData.管理费用下管理人员职工薪酬.getValue());
		

		
		if((fl+yjfl)>(zgxc+yfzgxc+xszgxc)*0.13){
			flag = true;
		}
		
		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	}
	
	
	public Map<String,Object> check07(Map<String,Object> map){
	
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
 		
		boolean flag = false;
		
		double ggf = (double) dataMap.get(FinanceAanalysisData.销售费用下广告费.getValue());
		double ywxcf = (double) dataMap.get(FinanceAanalysisData.销售费用下业务宣传费.getValue());
	
		double yysr = (double) dataMap.get(FinanceAanalysisData.利润表下年营业收入.getValue());
	
		if(ggf+ywxcf>yysr*0.13){
			flag = true;
		}
		

		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	}
	
	public double getYysrByIncomeQuarter(){
		
		double yysr = 0;

		
		if(period.endsWith("01")||period.endsWith("02")||period.endsWith("03")){
			yysr = (double) dataMap.get(FinanceAanalysisData.利润表季报表下第一季度营业收入.getValue());
		}
		if(period.endsWith("04")||period.endsWith("05")||period.endsWith("06")){
			yysr = (double) dataMap.get(FinanceAanalysisData.利润表季报表下第二季度营业收入.getValue());
		}
		if(period.endsWith("07")||period.endsWith("08")||period.endsWith("09")){
			yysr = (double) dataMap.get(FinanceAanalysisData.利润表季报表下第三季度营业收入.getValue());
		}
		if(period.endsWith("10")||period.endsWith("11")||period.endsWith("12")){
			yysr = (double) dataMap.get(FinanceAanalysisData.利润表季报表下第四季度营业收入.getValue());
		}
		
		
		return yysr;
	}
	
	public int getJdByIncomeQuarter(){
		
		int jd = 0;

		
		if(period.endsWith("01")||period.endsWith("02")||period.endsWith("03")){
			jd = 1;
		}
		if(period.endsWith("04")||period.endsWith("05")||period.endsWith("06")){
			jd = 2;
		}
		if(period.endsWith("07")||period.endsWith("08")||period.endsWith("09")){
			jd = 3;
		}
		if(period.endsWith("10")||period.endsWith("11")||period.endsWith("12")){
			jd = 4;
		}
		
		
		return jd;
	}
	public Map<String,Object> check08(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;
		double yysr = 0;
		yysr = getYysrByIncomeQuarter();

 		if(yysr>90000){
			flag = true;
		}
 		
		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
		
	    resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		 
		return resultMap;
	}
	
	public  Map<String,Object>  check09(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;
		double yysr = 0;
		yysr = getYysrByIncomeQuarter();
		
		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
		
 		if(yysr<90000){
			flag = true;
		}
 		//东家您好！您的企业广州***公司本季度收入"+yysr+"元,应交税费为…元,净利润为…元。如有疑问请拨打e账柜财税共享服务平台电话400-88-12580（一按我帮您）
		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		 return resultMap;
	}
	
	public Map<String,Object> check10(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		//东家您好！您的企业广州***公司目前代扣代缴个人所得税共计**元，可以向税局申请返还2%的手续费，共计**元，如需代办请拨打电话400-88-12580（一按我帮您）
		boolean flag = false;
		double yjsf = (double) dataMap.get(FinanceAanalysisData.应交税费下应交个人所得税.getValue());
		
		if(yjsf > 0){
			flag = true;
		}
		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
 		msg = msg.replace(":yjsf", yjsf+"");
 		msg = msg.replace(":fhsf", (yjsf*0.02)+"");
 	
 		 resultMap.put("msg", msg);
		 resultMap.putAll(map);
		 resultMap.put("flag", flag);
		 
		return resultMap;
	}
	
	
	public Map<String,Object> check11(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		//东家您好！您的企业广州***公司目前代扣代缴个人所得税共计**元，可以向税局申请返还2%的手续费，共计**元，如需代办请拨打电话400-88-12580（一按我帮您）
		boolean flag = false;
		double yjsf = (double) dataMap.get(FinanceAanalysisData.应交税费下应交个人所得税.getValue());
		
		if(yjsf > 0){
			flag = true;
		}
		
		String msg = (String) map.get("problem_description");
		msg = msg.replace(":companyName", companyName);
	
		resultMap.put("msg", msg);
		resultMap.putAll(map);
		resultMap.put("flag", flag);
		 
		return resultMap;
	}
	public Map<String,Object> check12(Map<String,Object> map){

		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;
	
		
		double wxzc = (double) dataMap.get(FinanceAanalysisData.无形资产当月.getValue());
		double uwxzc = (double) dataMap.get(FinanceAanalysisData.无形资产前一月.getValue());
		double ssgb = (double) dataMap.get(FinanceAanalysisData.实收资本当月.getValue());
		double ussgb =  (double) dataMap.get(FinanceAanalysisData.实收资本前一月.getValue());
		

		double jwxzc = wxzc -uwxzc;
		double jssgb = ssgb - ussgb;
		if(jwxzc >0 && jssgb >0 && jwxzc == jssgb){
			flag = true;
		}


		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
		
	    resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		 
		return resultMap;
	}
	
	public Map<String,Object> check13(Map<String,Object> map){
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;		
		double yhck  = (double) dataMap.get(FinanceAanalysisData.银行存款.getValue());
	
		if(yhck >500000){
			flag = true;
		}

		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
		
	    resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		 
		return resultMap;
	}
	
	public Map<String,Object> check14(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		
		boolean flag = false;
		double yszk = 0;
		double yysr = 0;
	    
		if(yszk>yysr*0.2){
			flag = true;
		}
		String msg = (String) map.get("problem_description");
 		msg = msg.replace(":companyName", companyName);
 		msg = msg.replace(":yszk", yszk+"");
		resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		 
		return resultMap;
	}
	
	public Map<String,Object> check15(Map<String,Object> map){
	
		Map<String,Object> resultMap = new HashMap<String,Object>();
		boolean flag = true;
		
	
		
		double yysr = getYysrByIncomeQuarter();
		int jd = getJdByIncomeQuarter();
		String msg = (String) map.get("problem_description");
		msg = msg.replace(":companyName", companyName);
 		msg = msg.replace(":yysr", yysr+"");
 		msg = msg.replace(":quarter", jd+"");
 		msg = msg.replace(":jnsf", (String) dataMap.get(FinanceAanalysisData.应缴当季度税费.getValue()));
		resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		return resultMap;
		
	}
	
	public Map<String,Object> check16(Map<String,Object> map){
		
		Map<String,Object> resultMap = new HashMap<String,Object>();
		boolean flag = true;
		
		double yysr = (double) dataMap.get(FinanceAanalysisData.利润表下月营业收入.getValue());
		
		String msg = (String) map.get("problem_description");
		msg = msg.replace(":companyName", companyName);
		msg = msg.replace(":month", Integer.valueOf(period.substring(4,6))+"");
 		msg = msg.replace(":yysr", yysr+"");
 		msg = msg.replace(":jnsf", (String) dataMap.get(FinanceAanalysisData.应缴税费.getValue()));
		resultMap.put("msg", msg);
	    resultMap.putAll(map);
	    resultMap.put("flag", flag);
		return resultMap;
		
	}
	
	
	public List<Map<String,Object>> beginCheck(String companyName,String accountBookId,String period, int i){
			
			//需要发送短信
			List<Map<String,Object>>  resultList = new ArrayList<Map<String,Object>>();
					
			YongYouDataDao dataDao = new YongYouDataDao();
			
			FinanceAnalysisDao financeDao = new FinanceAnalysisDao();
			
			List<Map<String,Object>> resultMaps = 	financeDao.getFinanceAnalysis(period.substring(4, period.length()),i);
			
			FinancAnalysisUtil util = new FinancAnalysisUtil(dataDao, financeDao, accountBookId,period);
		
			Map<Integer, Object> dataMap = util.getData();
		
			this.companyName = companyName;
			this.period = period;
			this.dataMap = dataMap;
			String year  = period.substring(0,4);
		
			for(Map<String,Object> resultMap : resultMaps){
				String methodName = (String) resultMap.get("equation");
				System.out.println(methodName);
				try { 
					Class[] clazzs  =	SetUtils.getMethodParamTypes(this, methodName);
					Method pom = this.getClass().getDeclaredMethod(methodName,
						clazzs);
					Map<String,Object> invokeMap =  (Map<String, Object>) pom.invoke(this,resultMap);
					
			
					boolean flag = (boolean) invokeMap.get("flag");
					
				
					if(flag  &&  (int)resultMap.get("frequency_type") != FrequencyType.不显示PC.getValue()){//表示需要处理
						
						
						resultList.add(invokeMap);
						
					}
		
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return resultList;
		
		
	}
	
	@SuppressWarnings("unchecked")
	public void beginCheck(String companyName,String phone,String accountBookId,String period,int taxType){//需要发送短信
	
		YongYouDataDao dataDao = new YongYouDataDao();
		
		FinanceAnalysisDao financeDao = new FinanceAnalysisDao();
		
		List<Map<String,Object>> resultMaps = 	financeDao.getFinanceAnalysis(period.substring(4, period.length()),taxType);
		
		FinancAnalysisUtil util = new FinancAnalysisUtil(dataDao, financeDao, accountBookId,period);
	
		Map<Integer, Object> dataMap = util.getData();
	
		this.companyName = companyName;
		this.period = period;
		this.dataMap = dataMap;
		String year  = period.substring(0,4);
	
		for(Map<String,Object> resultMap : resultMaps){
			String methodName = (String) resultMap.get("equation");
			System.out.println(methodName);
			try { 
				Class[] clazzs  =	SetUtils.getMethodParamTypes(this, methodName);
				Method pom = this.getClass().getDeclaredMethod(methodName,
					clazzs);
				Map<String, Object> invokeMap = (Map<String, Object>) pom.invoke(this,resultMap);
			
				
				//根据发送短信的类型来处理
				int count = 0;
				String analysisId = (String) resultMap.get("finance_analysis_id");
				int frequency = (int) resultMap.get("frequency");
				
				boolean flag = (boolean) invokeMap.get("flag");
				
				int analysisResult = 2;
				
				if(flag){//表示需要处理
					
					analysisResult = 1;
					if(FrequencyType.年.getValue() == (int)resultMap.get("frequency_type")){
						count = financeDao.getFinancAnalysisSendCountByPeriod(accountBookId,analysisId,year,1);
					}
					if(FrequencyType.月.getValue() == (int)resultMap.get("frequency_type")){
						count = financeDao.getFinancAnalysisSendCountByPeriod(accountBookId,analysisId,period,1);
					}
					if(count < frequency &&  (int)resultMap.get("frequency_type") != FrequencyType.不发短信.getValue()){//表示未发送，，可以发送
						//MsgUtil.getInstance().sendMsg(phone, (String) invokeMap.get("msg"));

					}
				}
	
				//新增发送日志
				financeDao.createFinancAnalysisLog( accountBookId,analysisId,period,analysisResult);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	
	}
	
	
	
	public static void main(String[] args) {
		
		new FinanceAnalysisService().beginCheck("广州嘉磊元新信息科技有限公司","13430311521","a031e2ab-b2da-11e6-94b7-28e347652f73", "201611",TaxType.一般纳税人.getValue());
	//MsgUtil.getInstance().sendMsg("13430311521", "‰");
	}
}
