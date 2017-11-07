package com.upsmart.ausync.awss3;

import com.upsmart.server.common.codec.Gzip;
import com.upsmart.server.common.codec.MD5;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by yuhang on 17-11-7.
 */
public class MD5CreateTest {

    public static void main(String[] args) throws Exception {
        String file = "/home/upsmart/works/tag1";
        String zFile = Gzip.fileCompress(file, false);
        String md5Code = MD5.fileMD5(zFile, 0);
        System.out.println(md5Code);
    }
}
