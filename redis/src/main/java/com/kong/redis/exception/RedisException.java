package com.kong.redis.exception;

/**
 * redis异常类
 * Created by kong on 2016/4/29.
 */
public class RedisException extends Exception {
    public RedisException(String message) {
        super(message);
    }

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }
}
