package com.upsmart.ausync.configuration;

import com.upsmart.server.common.definition.Pair;
import com.upsmart.server.configuration.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by yuhang on 17-5-16.
 */
public class ConfigurationHelper {

    private static Logger LOGGER = LoggerFactory.getLogger(ConfigurationHelper.class);
    static
    {
        WORK_MODEL = getWorkModel();
        HTTP_PORT = getHttpPort();

        SLAVE_LISTEN_PORT = getSlaveListenPort();
        SLAVE_AWS_BUCKET_NAME = getSlaveAWSBucketName();
        SLAVE_AWS_REGION = getSlaveAWSRegion();
        SLAVE_AWS_AUDIENCE_PATH = getSlaveAwsAudiencePath();
        SLAVE_HISTORY_LOG = getSlaveHistoryLog();
        SLAVE_LOCAL_AUDIENCE_PATH = getSlaveLocalAudiencePath();
        SLAVE_ADDRESSES = getSlaveAddresses();
        SLAVE_REDIS = getSlaveRedis();
    }


    public static final String WORK_MODEL;
    private static String getWorkModel()
    {
        String value = ConfigurationManager.getAppSetting("workModel", "master");
        LOGGER.info(String.format("WORK_MODEL:[%s]", value));
        return value;
    }

    public static final int HTTP_PORT;
    private static int getHttpPort()
    {
        String value = ConfigurationManager.getAppSetting("httpPort", "10080");
        LOGGER.info(String.format("HTTP_PORT:[%s]", value));
        return Integer.valueOf(value);
    }

    public static final int SLAVE_LISTEN_PORT;
    private static int getSlaveListenPort()
    {
        String value = ConfigurationManager.getAppSetting("slaveListenPort", "13001");
        LOGGER.info(String.format("SLAVE_LISTEN_PORT:[%s]", value));
        return Integer.valueOf(value);
    }

    public static final String SLAVE_AWS_BUCKET_NAME;
    private static String getSlaveAWSBucketName()
    {
        String value = ConfigurationManager.getAppSetting("slaveAWSBucketName", "upsmart-portrait-audience-analysis");
        LOGGER.info(String.format("SLAVE_AWS_BUCKET_NAME:[%s]", value));
        return value;
    }
    public static final String SLAVE_AWS_REGION;
    private static String getSlaveAWSRegion()
    {
        String value = ConfigurationManager.getAppSetting("slaveAWSRegion", "cn-north-1");
        LOGGER.info(String.format("SLAVE_AWS_REGION:[%s]", value));
        return value;
    }
    public static final String SLAVE_HISTORY_LOG;
    private static String getSlaveHistoryLog()
    {
        String value = ConfigurationManager.getAppSetting("slaveHistoryLog", "/tmp/");
        LOGGER.info(String.format("SLAVE_HISTORY_LOG:[%s]", value));
        return value;
    }
    public static final String SLAVE_AWS_AUDIENCE_PATH;
    private static String getSlaveAwsAudiencePath()
    {
        String value = ConfigurationManager.getAppSetting("slaveAWSAudiencePath", "upsmart-prod-audience-data/data/");
        LOGGER.info(String.format("SLAVE_AWS_AUDIENCE_PATH:[%s]", value));
        return value;
    }
    public static final String SLAVE_LOCAL_AUDIENCE_PATH;
    private static String getSlaveLocalAudiencePath()
    {
        String value = ConfigurationManager.getAppSetting("slaveLocalAudiencePath", "/home/upsmart/works/");
        LOGGER.info(String.format("SLAVE_LOCAL_AUDIENCE_PATH:[%s]", value));
        return value;
    }



    public static final HashMap <String, String> SLAVE_ADDRESSES;
    private static HashMap<String, String> getSlaveAddresses()
    {
        Pair<String, String>[] temp = ConfigurationManager.getSettings("slaveAddresses");
        HashMap <String, String> ret = new HashMap <>();
        if (temp != null) {
            for (Pair <String, String> dataSubDir : temp) {
                assert (dataSubDir.name != null);
                ret.put(dataSubDir.name.toUpperCase(), dataSubDir.value);
                LOGGER.info(String.format("SLAVE_ADDRESSES:[%s]-[%s]", dataSubDir.name, dataSubDir.value));
            }
        }
        return ret;
    }

    public static final HashMap <String, String> SLAVE_REDIS;
    private static HashMap<String, String> getSlaveRedis()
    {
        Pair<String, String>[] temp = ConfigurationManager.getSettings("slaveRedis");
        HashMap <String, String> ret = new HashMap <>();
        if (temp != null) {
            for (Pair <String, String> dataSubDir : temp) {
                assert (dataSubDir.name != null);
                ret.put(dataSubDir.name.toUpperCase(), dataSubDir.value);
                LOGGER.info(String.format("SLAVE_REDIS:[%s]-[%s]", dataSubDir.name, dataSubDir.value));
            }
        }
        return ret;
    }
}
