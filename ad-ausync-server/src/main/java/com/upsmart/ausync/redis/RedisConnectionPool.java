package com.upsmart.ausync.redis;

import redis.clients.jedis.JedisCluster;

/**
 * Created by yuhang on 17-10-9.
 */
public class RedisConnectionPool extends JedisCluster {
    public RedisConnectionPool(RedisInfo redisDB) {
        super(redisDB.getHostAndPort(),
                redisDB.getConnTimeout(),
                redisDB.getReadTimeout(),
                redisDB.getMaxRedirections(),
                redisDB.getPoolConfig());
    }
}
