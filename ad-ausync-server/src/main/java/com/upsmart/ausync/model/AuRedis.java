package com.upsmart.ausync.model;

import java.io.UnsupportedEncodingException;

/**
 * Created by yuhang on 17-11-7.
 */
public class AuRedis {
    public String deviceId;
    public byte[] deviceIdKey;
    public AuRedis(String deviceId) {
        this.deviceId = deviceId;
        try {
            deviceIdKey = deviceId.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
    }
}