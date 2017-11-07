package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.model.TagScoreInfo;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.model.AuTagRedis;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.utils.DateUtil;

import java.net.URISyntaxException;
import java.util.Date;

/**
 * Created by yuhang on 17-11-7.
 */
public class TagWrapper extends RedisWrapper<AuTagRedis>{

    public TagWrapper(int threadCount, ActionType at) throws URISyntaxException {
        super(threadCount, at);
    }

    @Override
    protected byte[] process(Audience au, AuTagRedis t) {
        if(null != au && null != t && null != t.tagScore) {
            au.lastViewTime = DateUtil.dateToLong(new Date());

            for(TagScoreInfo tsi : t.tagScore){
                if(null == tsi){
                    continue;
                }
                if (ActionType.UPDATE.equals(actionType)) {
                    au.addTagScore(tsi.tagId, tsi);
                } else {
                    au.delTagScore(tsi.tagId);
                }
            }
            return AudienceSerializer.buildToBytes(au);
        }
        return null;
    }
}
