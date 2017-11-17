package com.upsmart.ausync.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by yuhang on 17-11-17.
 */
public class RedisClusterPool extends JedisCluster {
    public RedisClusterPool(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout,
                            int maxAttempts, final GenericObjectPoolConfig poolConfig) throws URISyntaxException {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public Map<byte[], byte[]> get(final Set<byte[]> keys) {
        if(null == keys || keys.isEmpty()){
            return null;
        }
        byte[] firstKey = null;
        for(byte[] k : keys){
            firstKey = k;
            break;
        }
        final Map<byte[], byte[]> result = new HashMap<>();
        new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                Pipeline pipeline = null;
                try{
                    pipeline = connection.pipelined();
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
                        result.put(k, r.get());
                    }
                }
                finally {
                    if(null != pipeline){
                        try {
                            pipeline.close();
                        } catch (IOException e) {
                        }
                    }
                }
                return null;
            }
        }.runBinary(firstKey);
        return result;
    }

    public void set(final Map<byte[], byte[]> data) {
        if(null == data || data.isEmpty()){
            return ;
        }
        byte[] firstKey = null;
        for(byte[] k : data.keySet()){
            firstKey = k;
            break;
        }

        final Map<byte[], byte[]> result = new HashMap<>();
        new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                Pipeline pipeline = null;
                try{
                    pipeline = connection.pipelined();
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
                }
                return null;
            }
        }.runBinary(firstKey);
    }
}
