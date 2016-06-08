package com.kong.redis.util;

/**
 * Created by kong on 2016/4/29.
 */
public class StringUtil {
    public static boolean isNull(String str) {
        return str == null || "".equals(str.trim()) ? true : false;
    }
}
