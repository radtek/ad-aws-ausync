package com.upsmart.ausync.awss3;


import com.upsmart.server.common.codec.Gzip;
import com.upsmart.server.common.codec.MD5;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by yuhang on 17-11-7.
 */
public class MD5CreateTest {

    public static void main(String[] args) throws Exception {

//        byte[][] x = new byte[2][];
//        x[0] = "111".getBytes("UTF-8");
//        x[1] = "222".getBytes("UTF-8");
//        t(x.length, x);

        String file = "/home/upsmart/works/tag1";
        String zFile = Gzip.fileCompress(file, false);
        String md5Code = MD5.fileMD5(zFile, 0);
        System.out.println(md5Code);
    }

    @Test
    public void test() throws IOException, NoSuchAlgorithmException {
        String zfile = "/home/upsmart/works/people.gz";
        String md5Code = MD5.fileMD5(zfile, 0);
        System.out.println(md5Code);
    }
}
