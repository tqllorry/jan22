package util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年02月12日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class RedisUtil {

    private static JedisPool jedisPool = null;

    //初始化连接
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxWaitMillis(100 * 1000);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop05", 6379, 5000);
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }

    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.select(1);

        jedis.set("20190211_123", "123");
        jedis.set("20190212_123", "123");
        jedis.set("20190213_456", "456");

        System.out.println(jedis.exists("20190211_123"));
    }
}
