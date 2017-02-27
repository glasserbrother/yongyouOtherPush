package com.zgcfo.ezg.app;

import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zgcfo.ezg.app.data.YongYouDataGet;
import com.zgcfo.ezg.entity.yongyou.YongYouDataEntity;
import com.zgcfo.ezg.service.YongYouDoData;

public class YongYouOtherPushApp {
	
	private static int produceTaskSleepTime = 90*1000;//队列等待30秒空闲
	private static int produceTaskSleepSecond = 90;//队列等待30秒空闲
	private static int produceTaskMinNumber = 4;//最小8个线程
	private static int produceTaskMaxNumber = 6;//最大10个线程
	private static int queueDeep = 2;//允许队列深度2(等待)
	
	private synchronized int getQueueSize(Queue queue) {
		return queue.size();
	}
	
	public static void main(String[] args) {
		YongYouOtherPushApp yongYouApp = new YongYouOtherPushApp();
		String period = "";
		if (args.length > 0 ){
			period = args[0];
		}
		
		YongYouDoData app = new YongYouDoData();
		List<YongYouDataEntity> list = app.getYongYouDataList(period);
		if (null != list && list.size() > 0){
			System.out.println("list: "+list.size());
			YongYouDataEntity yyData;
			YongYouDataGet yyGet;
						
			ThreadPoolExecutor threadPoolGet = new ThreadPoolExecutor(produceTaskMinNumber, produceTaskMaxNumber, produceTaskSleepSecond,
					TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueDeep),
					new ThreadPoolExecutor.DiscardOldestPolicy());
			
			for (int i = 0; (i < list.size() ); i++) {
				try {
					
					// 产生一个任务，并将其加入到线程池
					System.out.println(new Date()+"------------------------执行第"+i+"个");
					yyData = list.get(i);
					System.out.println(yyData);
					
					yyGet = new YongYouDataGet(yyData.getLoginName(),yyData.getPwd()
							,yyData.getBookId(),yyData.getYongyouId(), yyData.getCurrMonth()
							,yyData.getTableType(),yyData.getSubjectId());
					
					threadPoolGet.execute(new YYThreadPoolOtherGetAndPush(yyGet));
					
					while ( yongYouApp.getQueueSize(threadPoolGet.getQueue()) >= queueDeep) {
						System.out.println("队列已满，等90秒再添加任务");
						try {
							Thread.sleep(produceTaskSleepTime);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				System.out.println(new Date()+"------------------------完成第"+i+"个");
			}
			
			
			System.out.println(new Date() +"全部完成--恭喜");
			threadPoolGet.shutdown();
		}else{
			System.out.println("list: null");
		}
		
		
	}
	

}
