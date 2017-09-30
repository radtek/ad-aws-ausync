package com.upsmart.ausync.awss3;

import java.util.Date;

/**
 * Created by yuhang on 17-9-30.
 */
public class AwsS3FileInfo {

    public String path;
    public long size;
    public Date lastModified;

    public String convertSize(){
        if(size > 1000000000){
            // 1GB
            return String.valueOf(size/1000000000)+"GB";
        }
        else if (size > 1000000){
            // 1MB
            return String.valueOf(size/1000000)+"MB";
        }
        else if (size > 1000){
            // 1KB
            return String.valueOf(size/1000)+"KB";
        }
        return String.valueOf(size);
    }
}
