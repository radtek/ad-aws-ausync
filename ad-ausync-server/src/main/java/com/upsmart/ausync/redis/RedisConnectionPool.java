package com.upsmart.ausync.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by yuhang on 17-10-9.
 */
public class RedisConnectionPool {

    private JedisPool jedisPool;
    private JedisCluster jedisCluster;
    private boolean isCluster;

    public RedisConnectionPool(RedisInfo redisDB) throws URISyntaxException {
        if(redisDB.getIp().indexOf("cluster") > 0
                && redisDB.getIp().indexOf("amazonaws") > 0){
            isCluster = true;
            jedisCluster = new JedisCluster(
                    redisDB.getHostAndPort(),
                    redisDB.getConnTimeout(),
                    redisDB.getReadTimeout(),
                    redisDB.getMaxRedirections(),
                    redisDB.getPoolConfig());
        }
        else{
            isCluster = false;
            jedisPool = new JedisPool(redisDB.getPoolConfig(), redisDB.getURI(),  redisDB.getConnTimeout(), redisDB.getReadTimeout());
        }
    }
    public void close() throws IOException {
        if(null != jedisPool){
            jedisPool.close();
        }
        if(null != jedisCluster){
            jedisCluster.close();
        }
    }

    public byte[] get(byte[] key){
        byte[] ret = null;
        if(isCluster){
            ret = jedisCluster.get(key);
        }
        else{
            Jedis jedis = jedisPool.getResource();
            ret = jedis.get(key);
            jedis.close();
        }
        return ret;
    }
    public void set(byte[] key, byte[] value){
        if(isCluster){
            jedisCluster.set(key, value);
        }
        else{
            Jedis jedis = jedisPool.getResource();
            jedis.set(key, value);
            jedis.close();
        }
    }
}
