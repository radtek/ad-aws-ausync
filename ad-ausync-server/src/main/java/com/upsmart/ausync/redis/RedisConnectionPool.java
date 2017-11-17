package com.upsmart.ausync.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by yuhang on 17-10-9.
 */
public class RedisConnectionPool {

    private JedisPool jedisPool;
    private RedisClusterPool jedisCluster;
    private boolean isCluster;

    public RedisConnectionPool(RedisInfo redisDB) throws URISyntaxException {
        if(redisDB.getIp().indexOf("cluster") > 0
                && redisDB.getIp().indexOf("amazonaws") > 0){
            isCluster = true;
            jedisCluster = new RedisClusterPool(
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

    public int getJedisClusterCRC16(byte[] key){
        if(isCluster){
            return JedisClusterCRC16.getSlot(key);
        }
        else{
            return 1;
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
    public Map<byte[], byte[]> get(final Set<byte[]> keys) {
        Map<byte[], byte[]> ret = new HashMap<>();
        if(isCluster){
            ret = jedisCluster.get(keys);
        }
        else{
            Jedis jedis = null;
            Pipeline pipeline = null;
            try{
                jedis = jedisPool.getResource();
                pipeline = jedis.pipelined();
                Map<byte[], Response<byte[]>> resps = new HashMap<>();
                for(byte[] key : keys){
                    Response<byte[]> resp = pipeline.get(key);
                    resps.put(key, resp);
                }
                pipeline.sync();

                Iterator<Map.Entry<byte[], Response<byte[]>>> iter = resps.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<byte[], Response<byte[]>> entry = iter.next();
                    byte[] k = entry.getKey();
                    Response<byte[]> r = entry.getValue();
                    ret.put(k, r.get());
                }
            }
            finally {
                if(null != pipeline){
                    try {
                        pipeline.close();
                    } catch (IOException e) {
                    }
                }
                if(null != jedis){
                    jedis.close();
                }
            }

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
    public void set(final Map<byte[], byte[]> data) {
        if(isCluster){
            jedisCluster.set(data);
        }
        else{
            Jedis jedis = null;
            Pipeline pipeline = null;
            try{
                jedis = jedisPool.getResource();
                pipeline = jedis.pipelined();
                Iterator<Map.Entry<byte[], byte[]>> iter = data.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = iter.next();
                    byte[] k = entry.getKey();
                    byte[] r = entry.getValue();
                    if(null != k && k.length > 0
                            && null != r && r.length > 0) {
                        pipeline.set(k, r);
                    }
                }
                pipeline.sync();
            }
            finally {
                if(null != pipeline){
                    try {
                        pipeline.close();
                    } catch (IOException e) {
                    }
                }
                if(null != jedis){
                    jedis.close();
                }
            }
        }
    }
}
