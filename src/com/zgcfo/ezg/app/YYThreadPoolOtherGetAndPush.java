package com.zgcfo.ezg.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.zgcfo.ezg.app.commond.YongYouCommondGet;
import com.zgcfo.ezg.app.constant.YongYouConstants;
import com.zgcfo.ezg.app.data.YongYouDataGet;
import com.zgcfo.ezg.entity.yongyou.DetailAccountReport;
import com.zgcfo.ezg.entity.yongyou.Subject;
import com.zgcfo.ezg.entity.yongyou.YongYouDataEntity;
import com.zgcfo.ezg.redis.JedisUtil;
import com.zgcfo.ezg.redis.ObjectUtil;
import com.zgcfo.ezg.util.MyFormat;
import com.zgcfo.ezg.util.RedisUtil;

public class YYThreadPoolOtherGetAndPush implements Runnable, Serializable {

	private static final long serialVersionUID = 0;
	private static int consumeTaskSleepTime = 7*2000;
	private static int singleTaskSleepTime = 1000;
	private static int arrayTaskSleepTime = 200;
	private YongYouDataGet yyGet;

	public YYThreadPoolOtherGetAndPush(YongYouDataGet yyGet) {
		super();
		this.yyGet = yyGet;
	}

public void loadArrayData(YongYouCommondGet yyCommondGet, List<Object> listObjs){
		
		byte[] redisKey;
		Object detailList;
		try {
			
			if (listObjs !=null && listObjs.size() > 0){
				Subject sbj;
				Integer yongyouId;
				for (Object obj : listObjs){
					
					sbj = (Subject) obj;
					yongyouId =sbj.getYongyouId();
					if (yongyouId != null){
						detailList = yyCommondGet.getSpecialAndGet(yongyouId.toString());
						redisKey = RedisUtil.getCommondRedisKey(YongYouConstants.DETAIL_ACCOUNT_REPORT_INT);
						JedisUtil.lpush(redisKey, ObjectUtil.objectToBytes(detailList));
						System.out.println(new Date()+"  get-----  ["+new String(redisKey)+"]  "+Thread.currentThread().getName()+"执行完成");
						
						Thread.sleep(arrayTaskSleepTime);
					}
					
				}
			}
			
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void loadSingleData(YongYouCommondGet yyCommondGet){

		
		
		Object obj = null;
		byte[] redisKey;
		byte[] objs;
		
		try {
			obj = yyCommondGet.getCommondAndGetList();
			if (obj != null){
				redisKey = yyCommondGet.getCommondRedisKey();
				objs = ObjectUtil.objectToBytes(obj);
				JedisUtil.lpush(redisKey, objs);
				
				if (yyCommondGet.getCommond() == YongYouConstants.SUBJECT_INT){
					//Object detailList = yyCommondGet.getSpecialAndGetList(obj);
					//redisKey = RedisUtil.getCommondRedisKey(YongYouConstants.DETAIL_ACCOUNT_REPORT_INT);
					//JedisUtil.lpush(redisKey, ObjectUtil.objectToBytes(detailList));
					List<Object> list = (List<Object>) obj;
					loadArrayData(yyCommondGet, list);
					
				}
				
				System.out.println(new Date()+"  get-----  ["+new String(redisKey)+"]  "+Thread.currentThread().getName()+"执行完成");
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}
	
	public void loadData() throws Exception{
		String tableType = yyGet.getTableType();
		if ("init".equals(tableType)){
			return;
		}
		final int commond = MyFormat.formatInt(YongYouConstants.YYOtherMAP.get(tableType));
		if (commond == -1){
			throw new Exception();
		}
		
		new Thread(new Runnable(){
			YongYouCommondGet yyCommondGet = new YongYouCommondGet(yyGet, commond);	
						public void run(){
							loadSingleData(yyCommondGet);
						}
					})
		.start(); 
		Thread.sleep(singleTaskSleepTime);
			
	}


	@Override
	public void run() {
		
		try {
			loadData();
			Thread.sleep(consumeTaskSleepTime);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
