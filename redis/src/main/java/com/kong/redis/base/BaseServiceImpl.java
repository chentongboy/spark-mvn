package com.kong.redis.base;

import com.kong.exception.RedisException;
import com.kong.pool.JedisPooler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * redis操作抽象实现类
 * Created by kong on 2016/4/29.
 */
public abstract class BaseServiceImpl<V extends Serializable> implements IBaseService<V> {

    private boolean autoClose = true;

    public boolean isAutoClose() {
        return autoClose;
    }

    public void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }

    private final JedisPool pool = JedisPooler.getPool();
    static ThreadLocal<Jedis> localJedis = new ThreadLocal<Jedis>();

    public Jedis getJedis() {
        if (localJedis.get() == null) {
            localJedis.set(pool.getResource());
        }
        return localJedis.get();
    }

    public void returnBrokenResource() {
        Jedis jedis = localJedis.get();
        if (jedis != null) {
            returnBrokenResource(jedis);
        }
    }

    public void returnBrokenResource(Jedis jedis) {
        pool.returnBrokenResource(jedis);
        localJedis.set(null);
    }

    public void returnResource() {
        Jedis jedis = localJedis.get();
        if (jedis != null && isAutoClose()) {
            localJedis.set(null);
            pool.returnResource(jedis);
        }
    }

    public void returnResource(boolean close) {
        setAutoClose(close);
        returnResource();
    }

    /**
     * 获取key的值
     *
     * @param key
     * @return
     * @throws RedisException
     */
    @Override
    public V get(String key) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            byte[] val = jedis.get(getKey(key));
            return byte2Object(val);
        } catch (Exception e) {
            setAutoClose(true);
            e.printStackTrace();
            returnBrokenResource();
            throw new RedisException("RedisServer:V get(String key)");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    @Override
    public List<V> getVList(String key) throws RedisException {
        if (!isLinkedToRedis())
            return null;
        Jedis jedis = null;
        byte[] val = null;
        try {
            jedis = getJedis();
            val = jedis.get(getKey(key));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
        } finally {
            if (jedis != null)
                returnResource();
        }
        return (List<V>) byte2Object(val);
    }

    private boolean isLinkedToRedis() {
        return true;
    }

    /**
     * 保存key的值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    @Override
    public void save(String key, V value) throws RedisException {
        if (value == null)
            return;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.set(getKey(key), object2Bytes(value));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:save");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 保存key的值并设置过期时间
     *
     * @param key
     * @param value
     * @param timeout
     * @throws RedisException
     */
    public void save(String key, V value, int timeout) throws RedisException {
        if (value == null)
            return;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.set(getKey(key), object2Bytes(value));
            jedis.expire(getKey(key), timeout);
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:save with timeout");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 设置key的值集合
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    @Override
    public void save(String key, List<V> value) throws RedisException {
        if (value == null)
            return;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.set(getKey(key), object2Bytes(value));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:save with list");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 设置key的值集合,并设置过期时间
     *
     * @param key
     * @param value
     * @param timeout
     * @throws RedisException
     */
    public void save(String key, List<V> value, int timeout) throws RedisException {
        if (value == null)
            return;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.set(getKey(key), object2Bytes(value));
            jedis.expire(getKey(key), timeout);
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:save with list and timeout");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 对象序列化
     *
     * @param value
     * @return
     */
    private byte[] object2Bytes(Object value) {
        if (value == null)
            return null;
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(arrayOutputStream);
            outputStream.writeObject(value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                arrayOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return arrayOutputStream.toByteArray();
    }

    /**
     * 对象逆向序列化
     *
     * @param val
     * @return
     */
    private V byte2Object(byte[] val) {
        if (val == null || val.length == 0)
            return null;
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(val));
            Object obj = inputStream.readObject();

            return (V) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 字符串转化byte数组
     *
     * @param key
     * @return
     * @throws RedisException
     */
    private byte[] getKey(String key) throws RedisException {
        return key.getBytes();
    }

    /**
     * 更新key的值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    @Override
    public void update(String key, V value) throws RedisException {
        save(key, value);
    }

    /**
     * 更新key的值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    @Override
    public void update(String key, List<V> value) throws RedisException {
        save(key, value);
    }

    /**
     * 移除key的值
     *
     * @param key
     * @throws RedisException
     */
    @Override
    public void remove(String key) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.del(getKey(key));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:remove");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 从变量 +1
     *
     * @param key
     * @return
     * @throws RedisException
     */
    @Override
    public Long increment(String key) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            Long val = jedis.incr(getKey(key));
            return val;
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:increment");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 从列表的头部添加值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    public void addHeadList(String key, V value) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.lpush(key.getBytes(), this.object2Bytes(value));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:addHeadList");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 从列表的尾部添加值
     *
     * @param key
     * @param value
     * @throws RedisException
     */
    public void addTailList(String key, V value) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.rpush(key.getBytes(), this.object2Bytes(value));
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:addHeadList");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 获取key的list中的start至end之间的元素
     *
     * @param key
     * @param start
     * @param end
     * @return
     * @throws RedisException
     */
    public List<byte[]> listObjRanges(String key, int start, int end) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            List<byte[]> values = jedis.lrange(getKey(key), start, end);
            return values;
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:listObjRanges");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 获取key为list的V对象集合
     *
     * @param key
     * @param start
     * @param end
     * @return
     * @throws RedisException
     */
    public List<V> listObjPage(String key, int start, int end) throws RedisException {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            List<byte[]> values = jedis.lrange(getKey(key), start, end);
            returnResource();
            List<V> result = new ArrayList<V>();
            if (values != null && values.size() > 0) {
                for (byte[] bytes : values) {
                    result.add(this.byte2Object(bytes));
                }
            }
            return result;
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:listObjPage");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * 在Set中添加值
     * @param key
     * @param member
     * @throws RedisException
     */
    public void addMemberToSet(String key,String member) throws RedisException{
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.sadd(key,member);
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:addMemberToSet");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }

    /**
     * pop集合中的元素
     * @param key
     * @param head
     * @throws RedisException
     */
    public V pop(String key,boolean head) throws RedisException{
        Jedis jedis = null;
        try {
            jedis = getJedis();
            byte[] bytes = head?jedis.lpop(getKey(key)):jedis.rpop(getKey(key));
            return this.byte2Object(bytes);
        } catch (Exception e) {
            setAutoClose(true);
            returnBrokenResource();
            throw new RedisException("RedisServer:pop");
        } finally {
            if (jedis != null)
                returnResource();
        }
    }
}
