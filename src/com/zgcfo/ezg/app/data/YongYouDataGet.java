package com.zgcfo.ezg.app.data;


import java.util.ArrayList;
import java.util.List;

import com.zgcfo.ezg.entity.yongyou.Balance;
import com.zgcfo.ezg.entity.yongyou.BalanceReport;
import com.zgcfo.ezg.entity.yongyou.CashReport;
import com.zgcfo.ezg.entity.yongyou.DetailAccountReport;
import com.zgcfo.ezg.entity.yongyou.IncomeQuarterReport;
import com.zgcfo.ezg.entity.yongyou.IncomeReport;
import com.zgcfo.ezg.entity.yongyou.Subject;
import com.zgcfo.ezg.entity.yongyou.TaxReport;
import com.zgcfo.ezg.service.YongYouService;
import com.zgcfo.ezg.util.LogUtil;

public class YongYouDataGet {
	
	private String accountantLoginName;	
	private String accountantPwd;
	private String accountBookId;
	private Integer yongyouBookId;
	private String period;
	private String tableType;
	private String subjectId;
	private YongYouService yongyouService;

	
	public String getAccountantLoginName() {
		return accountantLoginName;
	}


	public void setAccountantLoginName(String accountantLoginName) {
		this.accountantLoginName = accountantLoginName;
	}


	public String getAccountantPwd() {
		return accountantPwd;
	}


	public void setAccountantPwd(String accountantPwd) {
		this.accountantPwd = accountantPwd;
	}


	public String getAccountBookId() {
		return accountBookId;
	}


	public void setAccountBookId(String accountBookId) {
		this.accountBookId = accountBookId;
	}


	public Integer getYongyouBookId() {
		return yongyouBookId;
	}


	public void setYongyouBookId(Integer yongyouBookId) {
		this.yongyouBookId = yongyouBookId;
	}


	public String getPeriod() {
		return period;
	}


	public void setPeriod(String period) {
		this.period = period;
	}


	public YongYouService getYongyouService() {
		return yongyouService;
	}


	public void setYongyouService(YongYouService yongyouService) {
		this.yongyouService = yongyouService;
	}

	
	public YongYouDataGet(String accountantLoginName, String accountantPwd,
			String accountBookId, Integer yongyouBookId, String period,
			String tableType, String subjectId) throws Exception{
		super();
		this.accountantLoginName = accountantLoginName;
		this.accountantPwd = accountantPwd;
		this.accountBookId = accountBookId;
		this.yongyouBookId = yongyouBookId;
		this.period = period;
		this.tableType = tableType;
		this.subjectId = subjectId;
		init();
	}


	@Override
	public String toString() {
		return "YongYouDataGet [accountantLoginName=" + accountantLoginName
				+ ", accountantPwd=" + accountantPwd + ", accountBookId="
				+ accountBookId + ", yongyouBookId=" + yongyouBookId
				+ ", period=" + period + ", tableType=" + tableType
				+ ", subjectId=" + subjectId + ", yongyouService="
				+ yongyouService + "]";
	}


	public String getTableType() {
		return tableType;
	}


	public void setTableType(String tableType) {
		this.tableType = tableType;
	}


	public String getSubjectId() {
		return subjectId;
	}


	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	public void init()  throws Exception{
		yongyouService = new YongYouService();
		int count = 5;
		while (! yongyouService.initService(accountantLoginName, accountantPwd) && count > 0){
			try {
				System.out.println("登录失败，3秒后重试");
				count-- ;
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				throw e;
			}
		}
		//如果登录失败则需要记录
		if ( count < 1){
			System.out.println("登录5次失败,请查看日志文件");
			LogUtil.logErr("init", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "init");
		}
	}
	
	//开始导入现金流量表
	public List<CashReport> getCashReport() throws Exception{
		return getCashReport(yongyouBookId, period);
	}
	public List<CashReport> getCashReport(Integer yongyouBookId, String period) throws Exception{
		List<Object> cashObjs = null;
		//try{
			cashObjs = yongyouService.getCashReport(yongyouBookId, period);
		//}catch(Exception e){
		//	LogUtil.logErr("CashReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		
		List<CashReport> objs = null;
		if(null != cashObjs && cashObjs.size() > 0){
			objs = new ArrayList<CashReport>();
			Object obj;
			String uniqueId;
			CashReport cashReport;
			for(int i = 0;i<cashObjs.size(); i++ ){
				obj = cashObjs.get(i);
				cashReport = (CashReport) obj;
				uniqueId = accountBookId + period+i;
				cashReport.setUniqueId(uniqueId);
				cashReport.setPeriod(period);
				cashReport.setAccountBookId(accountBookId);
				objs.add(cashReport);
			}
		}else{
			LogUtil.logErr("CashReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "cashObjs");
		}
		return objs;
	}
	
	//开始导入利润表
	public List<IncomeReport> getIncomeReport() throws Exception{
		return getIncomeReport(yongyouBookId, period);
	}
	public List<IncomeReport> getIncomeReport(Integer yongyouBookId, String period) throws Exception{
		List<Object> incomeReports = null;
		//try{
			incomeReports = yongyouService.getIncomeReport(yongyouBookId, period);
		//}catch(Exception e){
		//	LogUtil.logErr("IncomeReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		
		List<IncomeReport> objs = null;
		if(incomeReports != null && incomeReports.size() > 0){
			objs = new ArrayList<IncomeReport>();
			IncomeReport incomeReport;
			String uniqueId;
			for(int i=0;i<incomeReports.size();i++){
				
				incomeReport = (IncomeReport) incomeReports.get(i);
				uniqueId = accountBookId + period+i;
				incomeReport.setUniqueId(uniqueId);
				incomeReport.setPeriod(period);
				incomeReport.setAccountBookId(accountBookId);
				objs.add(incomeReport);
				
			}
		}else{
			LogUtil.logErr("IncomeReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "incomeReports");
		}
		
		return objs;		
	}
	
	//开始导入资产负债表
	public List<BalanceReport> getBalanceReport()  throws Exception{
		return getBalanceReport(yongyouBookId, period);
	}
	public List<BalanceReport> getBalanceReport(Integer yongyouBookId, String period)  throws Exception{
		List<Object> balanceReports =null;
		//try{
			balanceReports = yongyouService.getBalanceReport(yongyouBookId, period);
		//}catch(Exception e){
		//	LogUtil.logErr("BalanceReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		List<BalanceReport> objs = null;
		if(balanceReports != null && balanceReports.size() > 0){
			objs = new ArrayList<BalanceReport>();
			BalanceReport balancReport;
			String uniqueId ;
			for(int i=0; i<balanceReports.size();i++){
				
				balancReport = (BalanceReport) balanceReports.get(i);
				
				uniqueId = accountBookId + period+i;
				balancReport.setUnique_id(uniqueId);
				balancReport.setPeriod(period);
				balancReport.setAccountBookId(accountBookId);
				objs.add(balancReport);
			}
		}else{
			LogUtil.logErr("BalanceReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "balanceReports");
		}
		return objs;
	}
	
	//开始导入科目
	public List<Subject> getSubjects()  throws Exception{
		return getSubjects(yongyouBookId, period);
	}
	public List<Subject> getSubjects(Integer yongyouBookId, String period)  throws Exception{
		List<Object> subjects = null;
		//try{
			subjects = yongyouService.getSubjects(yongyouBookId);
		//}catch(Exception e){
		//	LogUtil.logErr("Subject", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		List<Subject> objs = null;
		if(subjects != null && subjects.size() > 0){
			objs = new ArrayList<Subject>();
			
			Subject sbj;
			for(Object obj:subjects){
				
				sbj = (Subject) obj;
				sbj.setAccountBookId(accountBookId);
				sbj.setUniqueId(accountBookId+sbj.getYongyouId());
				objs.add(sbj);
			}
		}else{
			LogUtil.logErr("Subject", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "subjects");
		}
		return objs;
	}
	
	//开始导入余额表
	public List<Balance> getBalance()  throws Exception{
		return getBalance(yongyouBookId, period);
	}
	public List<Balance> getBalance(Integer yongyouBookId, String period)  throws Exception{
		List<Object> balances =null;
		//try{
			balances = yongyouService.getBalance(yongyouBookId, period, period);
		//}catch(Exception e){
		//	LogUtil.logErr("Balance", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		List<Balance> objs = null;
		if(balances != null && balances.size()>0){
			objs = new ArrayList<Balance>();
			Balance balance;
			String uniqueId;
			for(int i =0; i<balances.size();i++){
				balance = (Balance) balances.get(i);
				uniqueId =accountBookId+period+period+i;
				balance.setUniqueId(uniqueId);
				balance.setStartPeriod(period);
				balance.setEndPeriod(period);
				balance.setAccountBookId(accountBookId);
				objs.add(balance);
			}
		}else{
			LogUtil.logErr("Balance", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "balances");
		}
		return objs;
	}
	
	//开始导入利润季报表
	public List<IncomeQuarterReport> getIncomeQuarterReport()  throws Exception{
		return getIncomeQuarterReport(yongyouBookId, period);
	}
	public List<IncomeQuarterReport> getIncomeQuarterReport(Integer yongyouBookId, String period)  throws Exception{
		List<Object> incomeQuarters = null;
		//try{
			incomeQuarters = yongyouService.getIncomeQuarterReport(yongyouBookId, period);
		//}catch(Exception e){
		//	LogUtil.logErr("IncomeQuarterReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		List<IncomeQuarterReport> objs = null;
		if(incomeQuarters != null && incomeQuarters.size() > 0){
			objs = new ArrayList<IncomeQuarterReport>();
			IncomeQuarterReport incomeQuarterReport;
			String uniqueId;
			for(int i=0; i<incomeQuarters.size();i++){
				
				incomeQuarterReport = (IncomeQuarterReport) incomeQuarters.get(i);
				uniqueId = accountBookId+period+i;
				incomeQuarterReport.setAccountBookId(accountBookId);
				incomeQuarterReport.setUniqueId(uniqueId);
				incomeQuarterReport.setPeriod(period);
				objs.add(incomeQuarterReport);
			}
			
		}else{
			LogUtil.logErr("IncomeQuarterReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "incomeQuarters");
		}
		return objs;
	}

	//开始导入纳税统计表
	public List<TaxReport> getTaxReports()  throws Exception{
		return getTaxReports(yongyouBookId, period);
	}
	public List<TaxReport> getTaxReports(Integer yongyouBookId, String period)  throws Exception{
		List<Object> taxReports = null;
		//try{
			taxReports = yongyouService.getTaxReports(yongyouBookId, period);
		//}catch(Exception e){
		//	LogUtil.logErr("TaxReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, e.toString());
		//}
		List<TaxReport> objs = null;
		if(taxReports != null && taxReports.size() > 0){
			objs = new ArrayList<TaxReport>();
			TaxReport taxReport;
			String uniqueId;
			for(int i=0; i<taxReports.size();i++){
				taxReport = (TaxReport) taxReports.get(i);
				uniqueId = accountBookId+period+i;
				taxReport.setAccountBookId(accountBookId);
				taxReport.setUniqueId(uniqueId);
				taxReport.setPeriod(period);
				objs.add(taxReport);
			}
		}else{
			LogUtil.logErr("TaxReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period, "taxReports");
		}
		return objs;
	}
	
	//开始导入明细账
	public List<List<DetailAccountReport>> getDetailAccountReport(List<Object> list)  throws Exception{
		List<List<DetailAccountReport>> total = null;
		List<DetailAccountReport> subList;
		if (list !=null && list.size() > 0){
			total = new ArrayList<List<DetailAccountReport>>();
			for (int i = 0;i< list.size(); i++){
				Subject sbj = (Subject) list.get(i);
				sbj.getYongyouId();
				subList = getDetailAccountReport(yongyouBookId, period, sbj.getYongyouId()+"");
				total.add(subList);
			}
		}
		return total;
	}
	public List<DetailAccountReport> getDetailAccountReport()  throws Exception{
		return getDetailAccountReport(yongyouBookId, period, subjectId);
	}
	public List<DetailAccountReport> getDetailAccountReport(String subjectId)  throws Exception{
		return getDetailAccountReport(yongyouBookId, period, subjectId);
	}	
	
	public List<DetailAccountReport> getDetailAccountReport(Integer yongyouBookId, String period, String subjectId)  throws Exception{
		List<Object> detailAccounts = null;
		//try{
			detailAccounts = yongyouService.getDetailAccountReport(yongyouBookId, period, period, subjectId);
		//}catch(Exception e){
		//	LogUtil.logErr("DetailAccountReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period,subjectId, e.toString());
		//}
		List<DetailAccountReport> objs = null;
		if(detailAccounts != null && detailAccounts.size() > 0){
			objs = new ArrayList<DetailAccountReport>();
			DetailAccountReport detailAccount;
			String uniqueId;
			
			for(int i = 0;i<detailAccounts.size();i++){
				detailAccount = (DetailAccountReport) detailAccounts.get(i);
				uniqueId = accountBookId+subjectId+period+period+i;
				detailAccount.setStartPeriod(period);
				detailAccount.setEndPeriod(period);
				detailAccount.setUniqueId(uniqueId);
				detailAccount.setAccountBookId(accountBookId);
				detailAccount.setSubjectId(subjectId+"");
				objs.add(detailAccount);
			}
		}else{
			LogUtil.logErr("DetailAccountReport", accountantLoginName, accountantPwd, accountBookId, yongyouBookId, period,subjectId, "detailAccounts");
		}
		return objs;
	}		

}

