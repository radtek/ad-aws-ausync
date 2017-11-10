package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.model.ExtendInfo;
import com.upsmart.audienceproto.model.TagScoreInfo;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.model.AuTagRedis;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.utils.DateUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yuhang on 17-11-7.
 */
public class TagWrapper extends RedisWrapper<AuTagRedis>{

    private int count;
    private Map<String, Integer> map = new HashMap<>();

    public TagWrapper(int threadCount, ActionType at) throws URISyntaxException, IOException {
        super(threadCount, at);
        count = Constant.getTagIndex(ConfigurationHelper.SLAVE_TAG_INDEX_PATH, map);
    }

    @Override
    protected byte[] process(Audience au, AuTagRedis t) {
        if(null != au && null != t && null != t.tagScore) {
            au.lastViewTime = DateUtil.dateToLong(new Date());

            ExtendInfo extendInfo = au.getExtendInfo("tags");
            if(null == extendInfo || null == extendInfo.ext){
                extendInfo = new ExtendInfo();
                extendInfo.ext_id = "tags";
                au.addExtendInfo("tags", extendInfo);
            }
            if(null == extendInfo.ext || extendInfo.ext.length == 0){
                extendInfo.ext = new byte[count];
            }
            else if(extendInfo.ext.length < count){
                byte[] newArray = new byte[count];
                System.arraycopy(extendInfo.ext,0,newArray,0,extendInfo.ext.length);
                extendInfo.ext = newArray;
            }

            for(TagScoreInfo tsi : t.tagScore){
                if(null == tsi){
                    continue;
                }
                Integer index = map.get(tsi.tagId);
                if(null == index){
                    continue;
                }
                index = index - 1;
                if (ActionType.UPDATE.equals(actionType)) {
                    extendInfo.ext[index] = (byte)tsi.tagScore;
                } else {
                    extendInfo.ext[index] = 0;
                }
            }
            return AudienceSerializer.buildToBytes(au);
        }
        return null;
    }
//
//    public static void p(Audience au, AuTagRedis t) throws IOException {
//        Map<String, Integer> map = new HashMap<>();
//        int count = Constant.getTagIndex("/home/upsmart/works/projects/adserver/ad-ausync/ad-ausync-server/properties/local/tagindex", map);
//        if(null != au && null != t && null != t.tagScore) {
//            au.lastViewTime = DateUtil.dateToLong(new Date());
//
//            ExtendInfo extendInfo = au.getExtendInfo("tags");
//            if(null == extendInfo || null == extendInfo.ext){
//                extendInfo = new ExtendInfo();
//                extendInfo.ext_id = "tags";
//                au.addExtendInfo("tags", extendInfo);
//            }
//            if(null == extendInfo.ext || extendInfo.ext.length == 0){
//                extendInfo.ext = new byte[count];
//            }
//            else if(extendInfo.ext.length < count){
//                byte[] newArray = new byte[count];
//                System.arraycopy(extendInfo.ext,0,newArray,0,extendInfo.ext.length);
//                extendInfo.ext = newArray;
//            }
//
//            for(TagScoreInfo tsi : t.tagScore){
//                if(null == tsi){
//                    continue;
//                }
//                int index = map.get(tsi.tagId) - 1;
//                extendInfo.ext[index] = (byte)tsi.tagScore;
//            }
//
//        }
//        int a = 0;
//        a++;
//    }
//
//    public static void main(String[] args) throws IOException {
//        Audience au = new Audience();
//        ExtendInfo extendInfo = new ExtendInfo();
//        extendInfo.ext_id = "tags";
////        extendInfo.ext = new byte[80];
////        extendInfo.ext[1] = 11;
////        extendInfo.ext[55] = 55;
//        au.addExtendInfo("tags", extendInfo);
//
//        TagScoreInfo tsi1 = new TagScoreInfo();
//        tsi1.tagId = "210";
//        tsi1.tagScore = 23;
//        TagScoreInfo tsi2 = new TagScoreInfo();
//        tsi2.tagId = "12003";
//        tsi2.tagScore = 88;
//        AuTagRedis atr = new AuTagRedis("xxxxx");
//        atr.tagScore.add(tsi1);
//        atr.tagScore.add(tsi2);
//
//        p(au, atr);
//
//        byte[] b = AudienceSerializer.buildToBytes(au);
//        Audience a = AudienceSerializer.parseFrom(b);
//        System.out.println(a.equals(au));
//        System.out.println(a.toString());
//
//    }
}
