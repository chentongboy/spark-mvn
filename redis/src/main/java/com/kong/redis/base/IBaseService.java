package com.kong.redis.base;

import com.kong.exception.RedisException;

import java.io.Serializable;
import java.util.List;

/**
 * 基础操作接口
 * Created by kong on 2016/4/29.
 */
public interface IBaseService<V extends Serializable> {
    /**
     * 获取key对应的值
     *
     * @param key
     * @return
     * @throws RedisException
     */
    V get(String key) throws RedisException;

    /**
     * 设置key的值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    void save(String key, V value) throws RedisException;

    /**
     * 更新key的值
     * @param key
     * @param value
     * @throws RedisException
     */
    void update(String key, V value) throws RedisException;

    /**
     * 移除key
     * @param key
     * @throws RedisException
     */
    void remove(String key) throws RedisException;

    Long increment(String key) throws RedisException;

    /**
     * 获取key对应的值集合
     * @param key
     * @return
     * @throws RedisException
     */
    List<V> getVList(String key) throws RedisException;

    /**
     * 设置key的值集合
     * @param key
     * @param value
     * @throws RedisException
     */
    void save(String key, List<V> value) throws RedisException;

    /**
     * 更新key的值
     * @param key
     * @param value
     * @throws RedisException
     */
    void update(String key, List<V> value) throws RedisException;
}
