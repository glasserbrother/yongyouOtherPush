package com.zgcfo.ezg.redis;

 
public class TestRedisQuene {
	private static final byte[] redisKey = "abckey".getBytes();
    public static void main(String[] args) {
    	//init();
    	for(int i = 0;i< 3;i++){
    		pop();
    	}
        
    }
 
    private static void pop() {
        byte[] bytes = JedisUtil.rpop(redisKey);
        Message msg;
		try {
			msg = (Message) ObjectUtil.bytesToObject(bytes);
			if(msg != null){
	            System.out.println(msg.getId()+"   "+msg.getContent());
	        }
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
 
    private static void init() {
        Message msg1 = new Message(1, "内容11");
        Message msg2 = new Message(2, "内容12");
        Message msg3 = new Message(3, "内容13");
        try {
			JedisUtil.lpush(redisKey, ObjectUtil.objectToBytes(msg1));
			JedisUtil.lpush(redisKey, ObjectUtil.objectToBytes(msg2));	        
	        JedisUtil.lpush(redisKey, ObjectUtil.objectToBytes(msg3));
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        
    }
 
}
