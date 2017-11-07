package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.model.AuRedis;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.utils.DateUtil;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;

/**
 * Created by yuhang on 17-10-10.
 */
public class AudienceWrapper extends RedisWrapper<AuRedis> {

    private List<String> audienceIds;

    public AudienceWrapper(int threadCount, List<String> audienceIds, ActionType at) throws URISyntaxException {
        super(threadCount, at);
        this.audienceIds = audienceIds;
    }

    @Override
    protected byte[] process(Audience au, AuRedis t) {
        if(null != au) {
            au.lastViewTime = DateUtil.dateToLong(new Date());
            if (ActionType.UPDATE.equals(actionType)) {
                au.addTags(audienceIds);
            } else {
                au.delTags(audienceIds);
            }

            return AudienceSerializer.buildToBytes(au);
        }
        return null;
    }
}
