package com.kong.redis.exception;

/**
 * Created by kong on 2016/4/30.
 */
public class CacheException extends Exception {
    public CacheException(String message) {
        super(message);
    }

    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
