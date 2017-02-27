package com.zgcfo.ezg.service;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import com.zgcfo.ezg.entity.yongyou.Balance;
import com.zgcfo.ezg.entity.yongyou.BalanceReport;
import com.zgcfo.ezg.entity.yongyou.CashReport;
import com.zgcfo.ezg.entity.yongyou.DetailAccountReport;
import com.zgcfo.ezg.entity.yongyou.IncomeQuarterReport;
import com.zgcfo.ezg.entity.yongyou.IncomeReport;
import com.zgcfo.ezg.entity.yongyou.Subject;
import com.zgcfo.ezg.entity.yongyou.TaxReport;
import com.zgcfo.ezg.util.AppMySQLConn;
import com.zgcfo.ezg.util.MyFormat;

/**
 * 用友数据插入工具类
 * 
 * 
 * */
public class YongyouDataUtil {
	
	private AppMySQLConn conn;

	private PreparedStatement psIncome ;//利润表
	
	private PreparedStatement psBalanceReport ;//资产负债表
	
	private PreparedStatement psCash ;//现金流量表
	
	private PreparedStatement psBalance;//余额表
	
	private PreparedStatement psIncomeQuarter;
	
	private PreparedStatement psSubject;//科目
	
	private PreparedStatement psDetailAccount;//明细账
	
	private PreparedStatement psTax;
	
	private String accountantLoginName;
	
	private String accountantPwd;
		
	private YongyouDataUtil(){

	}
	
	public YongyouDataUtil(String accountantLoginName,String accountantPwd,AppMySQLConn conn ){
		this.accountantLoginName = accountantLoginName;
		this.accountantPwd = accountantPwd;	
		this.conn = conn;
		System.out.println("init开始初始化");
		init();
		System.out.println("init初始化完成");
	}

	public void init(){
		
		String sql ;
		
		
		
		sql = " insert into yzg_yy_income_report(income_report_id ,unique_id,period,account_book_id,item	,row	,year_cash	,last_year_cash	,year_cash_BD	,last_year_cash_BD	,month_cash	,month_cash_BD	,tag	,fold	,fold_flag	,fomular_detail	) values(uuid(),  ?,?,?,?,?,  ?,?,?,?,?,  ?,?,?,?,? )"; 
		psIncome = conn.prepareStatement(sql);
		
		sql = " insert into yzg_yy_balance_report(balance_report_id,	unique_id,	period,	account_book_id,	asset,	asset_row,	asset_end_balance,	asset_end_balanceBD,	asset_year_init_balance,	asset_year_init_balanceBD,	equity,	equity_row,	equity_end_balance,	equity_end_balanceBD,	equity_year_init_balance,	equity_year_init_balanceBD,	tag	,fold,	fold_flag,	fomular_detail_one,	fomular_detail_two) values(uuid(), ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?) ";
		psBalanceReport = conn.prepareStatement(sql);
		
		sql = " insert into  yzg_yy_cash_report(cash_report_id,unique_id,period,account_book_id,	item	,row	,year_cash,	year_cash_BD,	last_year_cash,	last_year_cash_BD,	month_cash	,month_cash_BD	,tag	,fold,	fold_flag	,fomular_detail	,edit_tag) values(uuid(), ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?  ,? ) ";
		psCash = conn.prepareStatement(sql);
		
		


		sql = "insert into  yzg_yy_balance(balance_id,unique_id,account_book_id,start_period,end_period	,balance_type	,summary,	assistant_no,	end_b_exception,	end_df_balance_org,";//9
		sql	+= "end_jf_balance_org	,current_period_df_balance_org	,current_period_jf_balance_org	,init_df_balance_org,	init_jf_balance_org	,currency	,unit,end_price,init_price,";//9
		sql	+= "	end_balance,period,direction,parent_no,assistant_account,has_assistant_account,leaf,end_year_df_balance,init_year_balance,end_year_jf_balance,end_df_balance,end_jf_balance,";
		sql	+= " year_df_accumulate,year_jf_accumulate,year_df_actual_balance,year_jf_actual_balance,year_df_balance,year_jf_balance,current_period_df_actual_balance,current_period_jf_actual_balance,";
		sql	+= "	current_period_df_balance,current_period_jf_balance,init_year_df_balance,init_year_jf_balance,init_df_balance,init_jf_balance,subject_name,subject_id,subject_no,subject_type)";
		sql	+= " values(uuid(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		psBalance = conn.prepareStatement(sql);
		
	
		
		sql = "INSERT INTO yzg_yy_subject (subject_id,yongyou_id, sub_type_code,subject_no,subject_text,dir,subject_leaf,account_book_id,unique_id)";
		sql += "	VALUES 	(uuid(),?,?,?,?,?,?,?,?)";
		psSubject = conn.prepareStatement(sql);
		
		sql = "	INSERT INTO yzg_yy_detail_account_report ";
	    sql += "(detail_account_id,start_period, end_period,account_book_id, unique_id,voucher_id,period, voucher_word_no, summary,";//8
	    sql += "direction, jf_balance, df_balance, balance, jf_balance_org, df_balance_org, balance_org, exchange_rate, jf_num, df_num, ";//10
	    sql += "balance_num, jf_price, df_price, balance_price, subject_id, subject_name, subject_code, first_name, page_no_view, group_count, ";//10
	    sql += "	measuring_unit, curr_symbol, is_assistant_account_self, leaf, unit_name)";//5
	    sql += "	VALUES(uuid(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		psDetailAccount = conn.prepareStatement(sql);

		sql = "INSERT INTO yzg_oa.yzg_yy_income_quarter_report (income_quarter_id, item,	ROW, 	year_cash, 	last_year_cash, year_cash_bd, 	last_year_cash_bd, ";//6
		sql += "one_quarter_cash, one_quarter_cash_bd, two_quarter_cash,	two_quarter_cash_bd,  three_quarter_cash_bd, ";	//5
		sql += "three_quarter_cash,four_quarter_cash_bd, four_quarter_cash,tag,	fold, 	fold_flag,fomular_detail,	color_flag,      ";//8
		sql += "account_book_id,	unique_id,period) "	;//3
		sql += "values (uuid(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	

		psIncomeQuarter = conn.prepareStatement(sql);
 
		sql = " INSERT INTO yzg_oa.yzg_yy_tax_report ";
		sql +=" (tax_report_id,account_book_id,	unique_id,yongyou_id, subject_id, ";//4
		sql +=" measuring_unit,is_accounting_num,_12,_11,_10,_9,_8,_7,_6,_5,";//10
		sql +="_4,_3,_2,_1,_year,subject_no, 	subject_name, period)     ";//8
		sql +="VALUES(uuid(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		psTax = conn.prepareStatement(sql);
	
		
	}
	
	/**
	 * 开始导入用友数据
	 * @param accountBookId 本地账簿唯一Id
	 * @param yongyouBookId 用友账簿Id
	 * @param period 期间
	 * 
	 * */
	public void beginInsert(String accountBookId,Integer yongyouBookId,String period) throws Exception{
		
		YongYouService yongyouService = new YongYouService();
		yongyouService.initService(accountantLoginName, accountantPwd);
		
		//开始导入现金流量表
		List<Object> cashObjs = yongyouService.getCashReport(yongyouBookId, period);
		
		
		if(cashObjs != null){
			
			for(int i=0;i<cashObjs.size();i++){
				
				CashReport cashReport = (CashReport) cashObjs.get(i);
				String uniqueId = accountBookId + period+i;
				cashReport.setUniqueId(uniqueId);
				cashReport.setPeriod(period);
				cashReport.setAccountBookId(accountBookId);
				saveCashReport(cashReport);
				
			}
		}
		
		try {
			psCash.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		//开始导入利润表
		List<Object> incomeReports = yongyouService.getIncomeReport(yongyouBookId, period);
		
		if(incomeReports != null){
			
			for(int i=0;i<incomeReports.size();i++){
					
					IncomeReport incomeReport = (IncomeReport) incomeReports.get(i);
					String uniqueId = accountBookId + period+i;
					incomeReport.setUniqueId(uniqueId);
					incomeReport.setPeriod(period);
					incomeReport.setAccountBookId(accountBookId);
					saveIncomeReport(incomeReport);
					
				}
		}
		
		try {
			psIncome.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		//开始导入资产负债表
		List<Object> balanceReports = yongyouService.getBalanceReport(yongyouBookId, period);
		
		if(balanceReports != null){
			
			for(int i=0; i<balanceReports.size();i++){
				
				BalanceReport balancReport = (BalanceReport) balanceReports.get(i);
				
				String uniqueId = accountBookId + period+i;
				balancReport.setUnique_id(uniqueId);
				balancReport.setPeriod(period);
				balancReport.setAccountBookId(accountBookId);
				saveBalanceReport(balancReport);
			}
		}
		
		try {
			psBalanceReport.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
	
		//开始导入科目
		List<Object> subjects = yongyouService.getSubjects(yongyouBookId);
		
		
		
		if(subjects != null ){
		
			for(Object obj:subjects){
				
				Subject sbj = (Subject) obj;
				sbj.setAccountBookId(accountBookId);
				sbj.setUniqueId(accountBookId+sbj.getYongyouId());
				saveSubject(sbj,yongyouService,yongyouBookId,period);
			}
		}
		
		try {
			psSubject.close();
			psDetailAccount.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		//开始导入余额表
			List<Object> balances = yongyouService.getBalance(yongyouBookId, period, period);
			
			if(balances != null){
				for(int i =0; i<balances.size();i++){
					Balance balance = (Balance) balances.get(i);
					String uniqueId =accountBookId+period+period+i;
					balance.setUniqueId(uniqueId);
					balance.setStartPeriod(period);
					balance.setEndPeriod(period);
					balance.setAccountBookId(accountBookId);
					saveBalance(balance);
				}
			}
			
			try {
				psBalance.close();
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//开始导入利润季报表
			List<Object> incomeQuarters = yongyouService.getIncomeQuarterReport(yongyouBookId, period);
			
			if(incomeQuarters != null){
				
				for(int i=0; i<incomeQuarters.size();i++){
					
					IncomeQuarterReport incomeQuarterReport = (IncomeQuarterReport) incomeQuarters.get(i);
					String uniqueId = accountBookId+period+i;
					incomeQuarterReport.setAccountBookId(accountBookId);
					incomeQuarterReport.setUniqueId(uniqueId);
					incomeQuarterReport.setPeriod(period);
					saveIncomeQuarterReport(incomeQuarterReport);
				}
				
			}
			try {
				psIncomeQuarter.close();
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			//开始导入纳税统计表
			List<Object> taxReports = yongyouService.getTaxReports(yongyouBookId, period);
			
			if(taxReports != null){
				
				for(int i=0; i<taxReports.size();i++){
					TaxReport taxReport = (TaxReport) taxReports.get(i);
					String uniqueId = accountBookId+period+i;
					taxReport.setAccountBookId(accountBookId);
					taxReport.setUniqueId(uniqueId);
					taxReport.setPeriod(period);
					saveTaxReport(taxReport);
				}
			}
			
			System.out.println("数据导出完成>>>");
		conn.close();
	}
	
	private void saveTaxReport(TaxReport taxReport) {
		
		int index = 1;
		try {

			psTax.setString(index++,taxReport.getAccountBookId());
			psTax.setString(index++,taxReport.getUniqueId());
			psTax.setString(index++,taxReport.getYongyouId());
			psTax.setString(index++,taxReport.getSubjectId());
			psTax.setString(index++,taxReport.getMeasuringUnit());
			psTax.setString(index++,taxReport.getIsaccountingNum());
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_12()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_11()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_10()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_9()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_8()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_7()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_6()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_5()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_4()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_3()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_2()));
			psTax.setDouble(index++,MyFormat.formatDouble(taxReport.get_1()));
			psTax.setString(index++,taxReport.get_year());
			psTax.setString(index++,taxReport.getSubjectNo());
			psTax.setString(index++,taxReport.getSubjectName());
			psTax.setString(index++,taxReport.getPeriod());
			psTax.executeUpdate();
		} catch (Exception e) {
			System.out.println("saveTaxReport:异常");
		}
		
		
	}

	private void saveIncomeQuarterReport(IncomeQuarterReport incomeQuarterReport) {
		int index = 1;
		try {
			
			psIncomeQuarter.setString(index++, incomeQuarterReport.getItem());
			psIncomeQuarter.setDouble(index++, incomeQuarterReport.getRow());
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getYearCash()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getLastYearCash()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getYearCashBD()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getLastYearCashBD()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getOneQuarterCash()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getOneQuarterCashBD()));		
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getTwoQuarterCash()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getTwoQuarterCashBD()));	
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getThreeQuarterCashBD()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getThreeQuarterCash()));	
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getFourQuarterCashBD()));
			psIncomeQuarter.setDouble(index++, 	MyFormat.formatDouble(incomeQuarterReport.getFourQuarterCash()));
			psIncomeQuarter.setString(index++, incomeQuarterReport.getTag());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getFold());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getFoldFlag());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getFomularDetail());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getColorFlag());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getAccountBookId());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getUniqueId());
			psIncomeQuarter.setString(index++, incomeQuarterReport.getPeriod());
			psIncomeQuarter.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("saveIncomeQuarterReport:异常");
		}
		
	}

	/**
	 * 保存subject
	 * @param yongyouService 
	 * @param yongyouBookId 
	 * @param period 
	 * */
	private void saveSubject(Subject sbj, YongYouService yongyouService, Integer yongyouBookId, String period) {
		 
		int index = 1;
		try {
			psSubject.setInt(index++, sbj.getYongyouId());
			psSubject.setString(index++, sbj.getSubTypeCode());
			psSubject.setString(index++, sbj.getSubjectNo());
			psSubject.setString(index++, sbj.getSubjectText());
			psSubject.setString(index++, sbj.getDir());		
			psSubject.setBoolean(index++, sbj.getSubjectLeaf());
			psSubject.setString(index++, sbj.getAccountBookId());
			psSubject.setString(index++, sbj.getUniqueId());
			psSubject.executeUpdate();
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("saveSubject:异常");
		}
		
		//保存明细账
		List<Object> detailAccounts=null;
		try {
			detailAccounts = yongyouService.getDetailAccountReport(yongyouBookId, period, period, sbj.getYongyouId()+"");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(detailAccounts != null){
		
			for(int i = 0;i<detailAccounts.size();i++){
				DetailAccountReport detailAccount = (DetailAccountReport) detailAccounts.get(i);
				String uniqueId = sbj.getAccountBookId()+sbj.getYongyouId()+period+period+i;
				detailAccount.setStartPeriod(period);
				detailAccount.setEndPeriod(period);
				detailAccount.setUniqueId(uniqueId);
				detailAccount.setAccountBookId(sbj.getAccountBookId());
				detailAccount.setSubjectId(sbj.getYongyouId()+"");
				saveDetailAccountReport(detailAccount);
			}
		}
		
		
	}
	
	
	/**
	 * 保存明细账
	 * */
	private void saveDetailAccountReport(DetailAccountReport detailAccount) {
		
		int index = 1;
		try {
			   
			psDetailAccount.setString(index++, detailAccount.getStartPeriod());
			psDetailAccount.setString(index++, detailAccount.getEndPeriod());
			psDetailAccount.setString(index++, detailAccount.getAccountBookId());
			psDetailAccount.setString(index++, detailAccount.getUniqueId());
			psDetailAccount.setString(index++, detailAccount.getVoucherId());
			psDetailAccount.setString(index++, detailAccount.getPeriod());
			psDetailAccount.setString(index++, detailAccount.getVoucherWordNo());
			psDetailAccount.setString(index++, detailAccount.getSummary());
			psDetailAccount.setString(index++, detailAccount.getDirection());
			psDetailAccount.setDouble(index++,  MyFormat.formatDouble(detailAccount.getJfBalance()));

			 
			psDetailAccount.setDouble(index++,  MyFormat.formatDouble(detailAccount.getDfBalance()));
			psDetailAccount.setString(index++, detailAccount.getBalance());
			psDetailAccount.setDouble(index++,  MyFormat.formatDouble(detailAccount.getJfBalanceOrg()));
			psDetailAccount.setDouble(index++, MyFormat.formatDouble(detailAccount.getDfBalanceOrg()));
			psDetailAccount.setDouble(index++, MyFormat.formatDouble(detailAccount.getBalanceOrg()));
			
			psDetailAccount.setString(index++, MyFormat.formatStr(detailAccount.getExchangeRate()));
			
			psDetailAccount.setString(index++, detailAccount.getJfNum());
			psDetailAccount.setString(index++, detailAccount.getDfNum());
			
	
			psDetailAccount.setString(index++, detailAccount.getBalanceNum());
			psDetailAccount.setDouble(index++, MyFormat.formatDouble(detailAccount.getJfPrice()));
			psDetailAccount.setDouble(index++, MyFormat.formatDouble(detailAccount.getDfPrice()));
			psDetailAccount.setDouble(index++, MyFormat.formatDouble(detailAccount.getBalancePrice()));
			psDetailAccount.setString(index++, detailAccount.getSubjectId());
			psDetailAccount.setString(index++, detailAccount.getSubjectName());
			psDetailAccount.setString(index++, detailAccount.getSubjectCode());
			psDetailAccount.setString(index++, detailAccount.getFirstName());
			psDetailAccount.setString(index++, detailAccount.getPageNoView());
			psDetailAccount.setString(index++, detailAccount.getGroupCount());
			psDetailAccount.setString(index++, detailAccount.getMeasuringUnit());
			psDetailAccount.setString(index++, detailAccount.getCurrSymbol());
			psDetailAccount.setString(index++, detailAccount.getIsAssistantAccountSelf());
			psDetailAccount.setString(index++, detailAccount.getLeaf());
			psDetailAccount.setString(index++, detailAccount.getUnitName());
	
			psDetailAccount.executeUpdate();
		} catch (Exception e) {
			
			System.out.println("saveDetailAccountReport:异常");
		}
			
		
	}

	/**
	 * 保存余额表
	 * 
	 * **/
	private void saveBalance(Balance balance) {
		int index =1;
		try {
			
	

			psBalance.setString(index++, MyFormat.formatStr(balance.getUniqueId()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getAccountBookId()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getStartPeriod()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getEndPeriod()));
			psBalance.setInt(index++, balance.getBalanceType()==null?1:balance.getBalanceType());
			psBalance.setString(index++, MyFormat.formatStr(balance.getSummary()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getAssistantNo()));
			psBalance.setBoolean(index++, balance.getEndBException());
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getEndDfbalanceOrg()));

			
		
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getEndJfbalanceOrg()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getCurrentPeriodDfBalanceOrg()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getCurrentPeriodJfBalanceOrg()));
			psBalance.setDouble(index++,MyFormat.formatDouble( balance.getInitDfBalanceOrg()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getInitJfBalanceOrg()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getCurrency()));	
			psBalance.setString(index++, MyFormat.formatStr(balance.getUnit()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getEndPrice()));
			psBalance.setDouble(index++,MyFormat.formatDouble( balance.getInitPrice()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getEndbalance()));
			
			psBalance.setString(index++, MyFormat.formatStr(balance.getPeriod()));
			
			psBalance.setString(index++, MyFormat.formatStr(balance.getDirection()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getParentNo()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getAssistantAccount()));
			psBalance.setBoolean(index++, balance.getHasAssistantAccount());
			psBalance.setBoolean(index++, balance.getLeaf());
			psBalance.setDouble(index++ ,MyFormat.formatDouble( balance.getEndYearDfbalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getInitYearBalance()));
			psBalance.setDouble(index++,MyFormat.formatDouble( balance.getEndYearJfbalance()));
			psBalance.setDouble(index++,MyFormat.formatDouble( balance.getEndDfbalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getEndJfbalance()));			
			psBalance.setDouble(index++ ,MyFormat.formatDouble( balance.getYearDfAccumulate()));
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getYearJfAccumulate()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getYearDfActualBalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getYearJfActualBalance()));		
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getYearDfBalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble(balance.getYearJfBalance()));
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getCurrentPeriodDfActualBalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getCurrentPeriodJfActualBalance()));						
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getCurrentPeriodDfBalance()));
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getCurrentPeriodJfBalance()));			
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getInitYearDfBalance()));
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getInitYearJfBalance()));					
			psBalance.setDouble(index++,  MyFormat.formatDouble(balance.getInitDfBalance()));
			psBalance.setDouble(index++, MyFormat.formatDouble( balance.getInitJfBalance()));			
			psBalance.setString(index++, MyFormat.formatStr(balance.getSubjectName()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getSubjectId()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getSubjectNo()));
			psBalance.setString(index++, MyFormat.formatStr(balance.getSubjectType()));
			psBalance.executeUpdate();
			
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("saveBalance:异常"+e);
		}
		
	}

	/**
	 * 保存资产负债表
	 * */
	private void saveBalanceReport(BalanceReport balancReport) {
		int index =1;
		try {
		
			psBalanceReport.setString(index++, balancReport.getUnique_id());
			psBalanceReport.setString(index++, balancReport.getPeriod());
			psBalanceReport.setString(index++, balancReport.getAccountBookId());
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getAsset()) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getAssetRow()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getAssetEndBalance()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getAssetEndBalanceBD()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getAssetYearInitBalance()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getAssetYearInitBalanceBD()) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getEquity()) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getEquityRow()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getEquityEndBalance()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getEquityEndBalanceBD()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getEquityYearInitBalance()) );
			psBalanceReport.setDouble(index++, MyFormat.formatDouble(balancReport.getEquityYearInitBalanceBD()) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getTag()	) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getFold()	) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getFoldFlag()	) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getFomularDetailOne() ) );
			psBalanceReport.setString(index++, MyFormat.formatStr(balancReport.getFomularDetailTwo() ) );
			psBalanceReport.executeUpdate();
	
		} catch (Exception e) {
			System.out.println("saveBalanceReport:异常");
		}
		
		
	}

	/***
	 * 保存利润表
	 * */
	private void saveIncomeReport(IncomeReport incomeReport) {
		int index = 1;
		try {
			psIncome.setString(index++, incomeReport.getUniqueId());
			psIncome.setString(index++, incomeReport.getPeriod());
			psIncome.setString(index++, incomeReport.getAccountBookId());
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getItem()) );
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getRow()) );
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getYearCash() ));
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getLastYearCash()) );
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getYearCashBD()) );
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getLastYearCashBD()) );
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getMonthCash()) );
			psIncome.setDouble(index++, MyFormat.formatDouble(incomeReport.getMonthCashBD()) );
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getTag()	) );
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getFold()	) );
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getFoldFlag()	) );
			psIncome.setString(index++, MyFormat.formatStr(incomeReport.getFomularDetail() ) );
			psIncome.executeUpdate();
		} catch (Exception e) {
			System.out.println("saveBalanceReport:异常");
		}
		
		
	}
	
	/**
	 * 保存现金流量表
	 * **/
	private void saveCashReport(CashReport cashReport) {
		// TODO Auto-generated method stub
		int index = 1;
		try {
			psCash.setString(index++, cashReport.getUniqueId());
			psCash.setString(index++, cashReport.getPeriod());
			psCash.setString(index++, cashReport.getAccountBookId());
			psCash.setString(index++, MyFormat.formatStr(cashReport.getItem()) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getRow()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getYearCash()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getYearCashBD()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getLastYearCash()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getLastYearCashBD()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getMonthCash()) );
			psCash.setDouble(index++, MyFormat.formatDouble(cashReport.getMonthCashBD()) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getTag()	) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getFold()	) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getFoldFlag()	) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getFomularDetail() ) );
			psCash.setString(index++, MyFormat.formatStr(cashReport.getEditTag()) );
			psCash.execute();
		
		} catch (Exception e) {
			System.out.println("saveCashReport:异常");
		}
		
	
		

	}


	public static void main(String[] args) {
		String uuid = "a031e2ab-b2da-11e6-94b7-28e347652f73";
		String uuid2 = "a032cd6c-b2da-11e6-94b7-28e347652f73";
		System.out.println(UUID.randomUUID().toString());
	

		YongyouDataUtil yongyouDataUtil = new YongyouDataUtil("15920417898", "ezg147258",new AppMySQLConn());
		try {
			yongyouDataUtil.beginInsert(uuid, 100014, "201611");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		/*YongyouDataUtil yongyouDataUtil2 = new YongyouDataUtil("15920417898", "ezg147258",new AppMySQLConn());
		
		yongyouDataUtil2.beginInsert(uuid, 100014, "201610");*/
		
	
	}
}
