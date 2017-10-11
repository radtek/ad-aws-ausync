package com.upsmart.ausync.process.slave;

import com.google.protobuf.InvalidProtocolBufferException;
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
import java.io.UnsupportedEncodingException;

/**
 * Created by yuhang on 17-10-10.
 */
public class AuTestProcessor implements HttpProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuTestProcessor.class);

    @Override
    public boolean process(HttpRequestWrapper request, HttpResponseWrapper response) {

        String resp = "";

        String deviceId = request.getParam("id");
        String work = request.getParam("work"); // 工作内容
        work = (null == work ? "" : work);


        try{
            switch (work){
                case "set":

                    break;
                case "show":
                default:
                    resp = show(deviceId);
                    break;
            }
        }
        catch (Exception ex){
            LOGGER.error("", ex);
            resp = ex.getCause().toString();
        }
        finally {

        }
        response.setStringData(resp);
        response.setStatus(HttpResponseStatus.OK);
        return true;
    }

    private String show(String deviceId) throws InvalidProtocolBufferException, UnsupportedEncodingException {
        RedisConnectionPool redisCluster = null;
        try {
            if (!StringUtil.isNullOrEmpty(deviceId)) {
                redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);

                byte[] key = deviceId.getBytes("UTF-8");
                byte[] data = redisCluster.get(key);
                if (null != data && data.length > 0) {
                    Audience au = AudienceSerializer.parseFrom(data);
                    return au.toString();
                }
            }
        }
        finally {
            if(null != redisCluster){
                try {
                    redisCluster.close();
                } catch (IOException e) {
                }
            }
        }
        return "no data";
    }
}
