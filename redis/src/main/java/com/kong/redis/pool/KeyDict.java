package com.kong.redis.pool;

/**
 * 
 * 缓存的Key管理器
 * Created by kong on 2016/4/30.
 *
 */
public class KeyDict {

	private static KeyDict instance = new KeyDict();

	
	private KeyDict() {

	}

	
	public static KeyDict getInstance() {
		if (instance == null) {
			instance = new KeyDict();
		}
		return instance;
	}

	
	private boolean checkKeyExists(String key) {
		return true;
	}

	
	public boolean registerKeyToDict(String key) {

		return true;
	}

}
