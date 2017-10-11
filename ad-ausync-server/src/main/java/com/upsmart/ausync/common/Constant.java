package com.upsmart.ausync.common;

import com.upsmart.server.common.uniquenumber.UniqueNumberGenerator;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

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
}
