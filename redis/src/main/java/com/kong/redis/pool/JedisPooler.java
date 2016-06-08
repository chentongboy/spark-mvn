package com.kong.redis.pool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * redis连接池
 * Created by kong on 2016/4/30.
 */
public class JedisPooler {

    private static JedisPool pool;
    private static String REDIS_HOST;
    private static String REDIS_PSW;
    private static int REDIS_PORT;
    private static int REDIS_MaxTotal;
    private static int REDIS_MaxIdle;
    private static int REDIS_MaxWait;

    static {
        Properties prop = new Properties();
        try {
            InputStream file = loadConfig("/jedisPool.properties");
            prop.load(file);
            REDIS_HOST = prop.getProperty("REDIS.HOST");
            REDIS_MaxTotal = Integer.parseInt(prop.getProperty("REDIS.MaxActive").trim());
            REDIS_MaxIdle = Integer.parseInt(prop.getProperty("REDIS.MaxIdle").trim());
            REDIS_MaxWait = Integer.parseInt(prop.getProperty("REDIS.MaxWait").trim());
            REDIS_PORT = Integer.parseInt(prop.getProperty("REDIS.PORT").trim());
            REDIS_PSW = prop.getProperty("REDIS.PSW");
        } catch (Exception e) {
            e.printStackTrace();
            throw new NullPointerException("jedisPool.properties is not found!");
        }

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(REDIS_MaxTotal);
        config.setMaxIdle(REDIS_MaxIdle);
        config.setMaxWaitMillis(REDIS_MaxWait);
        config.setTestOnBorrow(true);
        System.out.println("Cache服务信息：当前连接的服务IP为：" + REDIS_HOST + ":" + REDIS_PORT);
        if (REDIS_PSW == null || "".equals(REDIS_PSW.trim()))
            pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, 5000);
        else
            pool = new JedisPool(config, REDIS_HOST, REDIS_PORT, 5000, REDIS_PSW);
    }

    private static InputStream loadConfig(String configPath) throws Exception {
        InputStream file = null;
        try {
            String file1 = JedisPooler.class.getResource(configPath).getFile();
            file1 = URLDecoder.decode(file1);
            file = new FileInputStream(file1);
        } catch (Exception e) {
            System.out.println("读取配置文件中....");
            String currentJarPath = URLDecoder.decode(JedisPooler.class.getProtectionDomain().getCodeSource().getLocation().getFile(), "UTF-8");
            System.out.println("currentJarPath:" + currentJarPath);
            JarFile currentJar = new JarFile(currentJarPath);
            JarEntry entry = currentJar.getJarEntry("jedisPool.properties");
            InputStream in = currentJar.getInputStream(entry);
            file = in;
        }
        return file;
    }

    public static JedisPool getPool() {
        return pool;
    }

    public static void close(Jedis jedis) {
        if (jedis != null) {
            pool.returnResource(jedis);
        }
    }
}
