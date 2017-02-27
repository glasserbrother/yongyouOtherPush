package com.zgcfo.ezg.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;


/**
 * 对返回值进行解析级设值
 * */
public class SetUtils {
	private static SetUtils setUtils = null;

	public static SetUtils getInstance() {
		if (setUtils == null) {
			synchronized (SetUtils.class) {
				if (setUtils == null) {
					setUtils = new SetUtils();
				}
			}
		}
		return setUtils;
	}

	private SetUtils() {
	}
	


	public Object setValues(Map<String, Object> retMap, Object obj) throws NumberFormatException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		return setValues(retMap,obj,null);
	}
	public Object setValues(Map<String, Object> retMap, Object obj,Map<String,Object> replaceFileds)
			throws ClassNotFoundException, NumberFormatException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Iterator it = retMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String key = (String) entry.getKey();
			String value = String.valueOf(entry.getValue());
	
			String poMethodName = "set"
					+ ((String) key).substring(0, 1).toUpperCase()
					+ ((String) key).substring(1);
			
			if(replaceFileds.get(poMethodName) != null){
				poMethodName = (String) replaceFileds.get(poMethodName);
			}
		
		
			Class[] paramsTypes = this.getMethodParamTypes(obj, poMethodName);
			Method pom = null;
			
			try {
				
				pom = obj.getClass().getDeclaredMethod(poMethodName,
						paramsTypes);

			} catch (NoSuchMethodException e) {
				// TODO: handle exception
				
				/*System.out.println(key + "》》》》找不到对象对应的字段");*/
				continue;
			}
			
			try {

				switch (paramsTypes[0].getSimpleName()) {

				case "Double":
					if (value != null && !"".equals(value.trim())) {
						pom.invoke(obj, Double.valueOf(value + ""));
					}

					break;
				case "Long":

					if (value != null && !"".equals(value.trim())) {
						if("id".equals(key)){
							double d = Double.valueOf(value + "");
							Long dd =  (long) d;
							pom.invoke(obj,dd );
						}else{
							pom.invoke(obj, Long.valueOf(value + ""));
						}
						
					}

					break;
				case "long":

					if (value != null && !"".equals(value.trim())) {
						pom.invoke(obj, Long.valueOf(value + ""));
					}

					break;
				case "Integer":

					if (value != null && !"".equals(value.trim())) {
						if("count".equals(key)){
							double d = Double.valueOf(value + "");
							int dd = (int) d;
							pom.invoke(obj,dd );
						}else{
							pom.invoke(obj, Integer.valueOf(value + ""));
						}
					
					}

					break;
				case "int":

					if (value != null && !"".equals(value.trim())) {
						pom.invoke(obj, Integer.valueOf(value + ""));
					}

					break;
				case "Boolean":
					if (value != null && !"".equals(value.trim())) {
						pom.invoke(obj, Boolean.valueOf(value.toString()));
					}

					break;
				case "String":
					if (value != null && !"".equals(value.trim())) {

						pom.invoke(obj, value + "");
					}

					break;

				default:
					pom.invoke(obj, value);
					break;
				}
			} catch (Exception e) {
			/*	System.out.println("类型异常字段：" + key + "<>现在的类型:"
						+ paramsTypes[0].getSimpleName() + "<>现在的值:" + value);*/
			}
		}
		return obj;
	}
	public Object setValues(JSONObject retMap, Object obj) throws NumberFormatException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		return setValues(retMap,obj,null);
	}
	
	public Object setValues(JSONObject retMap, Object obj,Map<String,Object> replaceFiled)
			throws ClassNotFoundException, NumberFormatException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Map<String, Object> map = retMap;
		return setValues(map, obj,replaceFiled);
	}

	/** 得到方法参数类型 ***/
	@SuppressWarnings("rawtypes")
	public static Class[] getMethodParamTypes(Object classInstance,
			String methodName) throws ClassNotFoundException {
		Class[] paramTypes = null;
		Method[] methods = classInstance.getClass().getMethods();// 全部方法
		for (int i = 0; i < methods.length; i++) {
			if (methodName.equals(methods[i].getName())) {// 和传入方法名匹配
				Class[] params = methods[i].getParameterTypes();
				paramTypes = new Class[params.length];
				for (int j = 0; j < params.length; j++) {
					if (params[j].getName().equals("Integer")) {
						paramTypes[j] = Integer.class;
					} else if (params[j].getName().equals("String")) {
						paramTypes[j] = String.class;
					} else if (params[j].getName().equals("Double")) {
						paramTypes[j] = Double.class;
					} else if (params[j].getName().equals("Short")) {
						paramTypes[j] = Short.class;
					} else if (params[j].getName().equals("Long")) {
						paramTypes[j] = Long.class;
					} else if (params[j].getName().equals("long")) {
						paramTypes[j] = long.class;
					} else if (params[j].getName().equals("Float")) {
						paramTypes[j] = Float.class;
					} else if (params[j].getName().equals("Byte")) {
						paramTypes[j] = Byte.class;
					} else if (params[j].getName().equals("Boolean")) {
						paramTypes[j] = Boolean.class;
					} else {
						paramTypes[j] = Class.forName(params[j].getName());
					}
				}
				break;
			}
		}
		return paramTypes;
	}
	
}
