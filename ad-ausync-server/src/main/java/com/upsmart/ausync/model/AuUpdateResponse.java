package com.upsmart.ausync.model;

import com.upsmart.server.common.codec.Gzip;
import com.upsmart.server.common.utils.GsonUtil;

/**
 * Created by yuhang on 17-9-28.
 */
public class AuUpdateResponse {

    public String resCode;

    public String taskId;

    public String errMsg;

    public String serializeJsonToStr(){
        GsonUtil gson = new GsonUtil();
        return gson.serialize(this);
    }
    public byte[] serializeJsonToByte(){
        String str = serializeJsonToStr();
        return Gzip.compressToByte(str, "utf-8");
    }

    public static AuUpdateRequest deserialize(String str){
        GsonUtil gson = new GsonUtil();
        return gson.deserialize(str, AuUpdateRequest.class);
    }
    public static AuUpdateRequest deserialize(byte[] bytes){
        String str = Gzip.uncompressToString(bytes, "utf-8");
        return deserialize(str);
    }
}
