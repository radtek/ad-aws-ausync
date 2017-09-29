package com.upsmart.ausync.common;

import com.upsmart.server.common.uniquenumber.UniqueNumberGenerator;

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
}
