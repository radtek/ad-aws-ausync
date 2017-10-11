package com.upsmart.ausync.awss3;

import com.upsmart.server.common.codec.MD5;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created by yuhang on 17-10-11.
 */
public class MappedByteBufferTest {

    public static String toHex(byte b) {
        String result = Integer.toHexString(b & 0xFF);
        if (result.length() == 1) {
            result = '0' + result;
        }
        return result;
    }

    /**
     * 使用 vim -b [文件] 读取文件
     * 在使用 :%!xxd 查看16进制
     * @throws IOException
     */

    @Test
    public void test() throws IOException {
        String filePath = "/home/upsmart/works/170913/222";
        File file = new File(filePath);
        if (!file.isDirectory()) {
            FileInputStream in = null;
            FileChannel channel = null;
            try {
                in = new FileInputStream(file);
                channel = in.getChannel();
                final MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());

                boolean isNewLine = true;
                int index = 0;
                byte[] device = null;
                int bufferLength = byteBuffer.limit();
                for(int i=0; i<bufferLength; i++){
                    if(isNewLine){
                        device = new byte[1024];
                        index=0;
                        isNewLine = false;
                    }
                    byte b = byteBuffer.get();
                    String h = toHex(b);
                    byte n = '\n';
                    if(b == '\n'){
                        device[index++] = '\0';
                        isNewLine = true;
                        String line = new String(device);
                        System.out.println(line);
                    }
                    else{
                        device[index++] = b;
                    }
                }

                // 释放内存
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        try {
                            Method getCleanerMethod = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
                            getCleanerMethod.setAccessible(true);
                            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(byteBuffer, new Object[0]);
                            cleaner.clean();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                });

            }
            catch (Exception ex){
                ex.printStackTrace();
            }
            finally {
                if(null != channel){
                    channel.close();
                }
                if (null != in) {
                    in.close();
                }
            }
        }
    }
}
