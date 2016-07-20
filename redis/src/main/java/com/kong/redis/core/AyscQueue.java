package com.kong.redis.core;

import com.kong.redis.exception.CacheException;

import java.io.Serializable;

/**
 * 消费者队列接口
 * Created by kong on 2016/4/30.
 */
public interface AyscQueue extends Serializable{

    String AyscQueueKey = "Share:AyscQueue";

    Object popAyscQueue() throws CacheException;
}
