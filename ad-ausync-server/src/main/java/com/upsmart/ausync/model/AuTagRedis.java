package com.upsmart.ausync.model;

import com.upsmart.audienceproto.model.TagScoreInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuhang on 17-11-7.
 */
public class AuTagRedis extends AuRedis{

    public List<TagScoreInfo> tagScore = new ArrayList<>();

    public AuTagRedis(String deviceId) {
        super(deviceId);
    }
}
