package com.upsmart.ausync.model;

import com.upsmart.server.common.codec.Gzip;
import com.upsmart.server.common.utils.GsonUtil;

/**
 * Created by yuhang on 17-9-29.
 */
public abstract class SerializeBase {

    public String serializeJson(){
        GsonUtil gson = new GsonUtil();
        return gson.serialize(this);
    }
    public byte[] serializeJsonToGzip(){
        String str = serializeJson();
        return Gzip.compressToByte(str, "utf-8");
    }

    public SerializeBase deserialize(String str){
        GsonUtil gson = new GsonUtil();
        return gson.deserialize(str, this.getClass());
    }
    public SerializeBase deserializeFromGzip(byte[] bytes){
        String str = Gzip.uncompressToString(bytes, "utf-8");
        return deserialize(str);
    }
}
