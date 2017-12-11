package com.upsmart.ausync.common;

import com.upsmart.ausync.model.TransData;
import com.upsmart.server.common.uniquenumber.UniqueNumberGenerator;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yuhang on 17-3-31.
 */
public final class Constant {

    public static final String WORK_MODEL_MASTER = "master";
    public static final String WORK_MODEL_SLAVE = "slave";

    public static final String DATE_FORMAT_TOTAL = "yyyy-MM-dd HH:mm:ss";

    public static final String REGEX_SYMBOL_VERTICAL = "\\|";

    public static final String SYMBOL_VERTICAL = "|";
    public final static String SYMBOL_COLON = ":";
    public static final String SYMBOL_DOUBLE_COLON = "::";
    public static final String SYMBOL_COMMA = ",";
    public final static String SYMBOL_WILDCARD = "*";

    public final static String SYMBOL_ATAT = "@@";

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public final static long MAX_MAPPING_BUFF_SIZE = Integer.MAX_VALUE / 100;

    public static <K, V> int getRandomIndex(Map<K, V> map){

        int size = map.size();
        long rd = Math.abs(UniqueNumberGenerator.getInstance(20170313).next());
        return (int)(rd%size);
    }

    /**
     * 释放内存映射文件
     * @param byteBuffer
     */
    public static void releaseBuff(final MappedByteBuffer byteBuffer){

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

    /**
     * 获得tag标签的索引下标
     * @param path 索引文件路径,索引格式: id，序号
     * @param map
     * @return
     */
    public static int getTagIndex(String path, Map<String, Integer> map) throws IOException {
        if(null == map){
            return -1;
        }
        int ret = -1;

        File file = new File(path);
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try{
            inputStreamReader = new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-8");
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] arr = line.split(Constant.SYMBOL_COMMA);
                if(null != arr && arr.length >=2) {
                    String id = arr[0];
                    Integer index = Integer.valueOf(arr[1]);
                    map.put(id, index);
                    if(index > ret){
                        ret = index;
                    }
                }
            }
        }
        finally {
            if(null != bufferedReader){
                bufferedReader.close();
            }
            if(null != inputStreamReader){
                inputStreamReader.close();
            }
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {

        Map<String, Integer> map = new HashMap<>();
        int count = Constant.getTagIndex("/home/upsmart/works/projects/adserver/ad-ausync/ad-ausync-server/properties/local/tagindex", map);

        System.out.println(count);
    }
}
