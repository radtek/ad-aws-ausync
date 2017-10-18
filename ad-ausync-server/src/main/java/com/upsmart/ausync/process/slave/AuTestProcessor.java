package com.upsmart.ausync.process.slave;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.hang.netty.processor.HttpProcessor;
import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.model.CampaignInfo;
import com.upsmart.audienceproto.model.FrequencyInfo;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.redis.RedisConnectionPool;
import com.upsmart.ausync.redis.RedisInfo;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.StringUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yuhang on 17-10-10.
 */
public class AuTestProcessor implements HttpProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuTestProcessor.class);

    @Override
    public boolean process(HttpRequestWrapper request, HttpResponseWrapper response) {

        String resp = "";
        String work = request.getParam("work"); // 工作内容
        work = (null == work ? "" : work);

        try{
            switch (work){
                case "setfreq":
                    resp = setFreq(request);
                    break;
                case "settag":
                    resp = setTag(request);
                    break;
                default:
                    resp = show(request); // http://52.80.7.153:13800/audience/test?id=a76def8c-36d4-4850-ab1a-80745357291f
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

    public static void main(String[] args) throws InvalidProtocolBufferException, UnsupportedEncodingException {
        AuTestProcessor ap = new AuTestProcessor();
        ap.setFreq(null);
    }

    private String setTag(HttpRequestWrapper request) throws InvalidProtocolBufferException, UnsupportedEncodingException{
        String deviceId = request.getParam("id");
        String tag = request.getParam("tag");
        String del = request.getParam("del");
        RedisConnectionPool redisCluster = null;
        Audience au = null;
        try {
            if (!StringUtil.isNullOrEmpty(deviceId) && !StringUtil.isNullOrEmpty(tag)) {
                redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);

                byte[] key = deviceId.getBytes("UTF-8");
                byte[] data = null;
                data = redisCluster.get(key);
                if (null != data && data.length > 0) {
                    au = AudienceSerializer.parseFrom(data);
                }
                else{
                    au = new Audience();
                    au.version = 1;
                }
                au.lastViewTime = DateUtil.dateToLong(new Date());

                if(StringUtil.isNullOrEmpty(del)) {
                    au.addTag(tag);
                }
                else{
                    au.delTag(tag);
                }

                byte[] result = AudienceSerializer.buildToBytes(au);
                if(null != result) {
                    redisCluster.set(key, result);
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
        return (null == au ? "no data" : au.toString());
    }

    private String setFreq(HttpRequestWrapper request) throws InvalidProtocolBufferException, UnsupportedEncodingException{

        String deviceId = request.getParam("id");
        String campaignId = request.getParam("cpid");
        String period = request.getParam("period");
        String type = request.getParam("type");
        String count = request.getParam("count");
        String starttime = request.getParam("starttime");
        String del = request.getParam("del");

        RedisConnectionPool redisCluster = null;
        Audience au = null;
        try {
            if (!StringUtil.isNullOrEmpty(deviceId) && !StringUtil.isNullOrEmpty(campaignId)
                    && !StringUtil.isNullOrEmpty(period) && !StringUtil.isNullOrEmpty(type)) {
                redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);

                byte[] key = deviceId.getBytes("UTF-8");
                byte[] data = null;
                data = redisCluster.get(key);
                if (null != data && data.length > 0) {
                    au = AudienceSerializer.parseFrom(data);
                }
                else{
                    au = new Audience();
                    au.version = 1;
                }
                au.lastViewTime = DateUtil.dateToLong(new Date());

                CampaignInfo campaignInfo = au.getCampaignInfo(campaignId);
                if(null == campaignInfo){
                    campaignInfo = new CampaignInfo(campaignId);
                    au.addCampaignInfo(campaignId, campaignInfo);
                }
                if(StringUtil.isNullOrEmpty(del)){
                    FrequencyInfo frequencyInfo = campaignInfo.getFrequency(Integer.valueOf(period), Integer.valueOf(type));
                    if(null == frequencyInfo){
                        frequencyInfo = new FrequencyInfo(Integer.valueOf(period), Integer.valueOf(type));
                        campaignInfo.addFrequency(frequencyInfo);
                    }

                    frequencyInfo.count = (StringUtil.isNullOrEmpty(count) ? 3 : Integer.valueOf(count));
                    frequencyInfo.startTime = (StringUtil.isNullOrEmpty(starttime) ? DateUtil.dateToLong(new Date()) : DateUtil.dateToLong(DateUtil.parseDate(starttime, "yyyyMMddHHmmss")) );

                }
                else{
                    FrequencyInfo frequencyInfo = new FrequencyInfo(Integer.valueOf(period), Integer.valueOf(type));
                    campaignInfo.delFrequency(frequencyInfo);
                }

                byte[] result = AudienceSerializer.buildToBytes(au);
                if(null != result) {
                    redisCluster.set(key, result);
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
        return (null == au ? "no data" : au.toString());
    }

    private String show(HttpRequestWrapper request) throws InvalidProtocolBufferException, UnsupportedEncodingException {
        String deviceId = request.getParam("id");
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
