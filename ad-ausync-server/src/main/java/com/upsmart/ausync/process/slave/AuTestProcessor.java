package com.upsmart.ausync.process.slave;

import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.hang.netty.processor.HttpProcessor;
import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.redis.RedisConnectionPool;
import com.upsmart.ausync.redis.RedisInfo;
import com.upsmart.server.common.utils.StringUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yuhang on 17-10-10.
 */
public class AuTestProcessor implements HttpProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuTestProcessor.class);

    @Override
    public boolean process(HttpRequestWrapper request, HttpResponseWrapper response) {

        String resp = "";

        String deviceId = request.getParam("id");
        RedisConnectionPool redisCluster = null;
        try{
            if(!StringUtil.isNullOrEmpty(deviceId)){
                redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);

                byte[] key = deviceId.getBytes("UTF-8");
                byte[] data = redisCluster.get(key);
                if(null != data && data.length > 0) {
                    Audience au = AudienceSerializer.parseFrom(data);
                    resp = au.toString();
                }
                else{
                    resp = "no data";
                }
            }
        }
        catch (Exception ex){
            LOGGER.error("", ex);
            resp = ex.getCause().toString();
        }
        finally {
            if(null != redisCluster){
                try {
                    redisCluster.close();
                } catch (IOException e) {
                }
            }
        }
        response.setStringData(resp);
        response.setStatus(HttpResponseStatus.OK);
        return true;
    }
}
