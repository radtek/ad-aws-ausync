package com.upsmart.ausync.core;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.model.WorkQueue;
import com.upsmart.ausync.process.slave.AcceptCmd;
import com.upsmart.ausync.process.slave.AudienceFileProcessor;
import com.upsmart.ausync.redis.RedisConnectionPool;
import com.upsmart.ausync.redis.RedisInfo;
import com.upsmart.server.common.utils.GsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yuhang on 17-5-15.
 */
public final class Environment {

    private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);

    private static WorkQueue workQueue;
    private static RedisConnectionPool redisPool;
    public static WorkQueue getWorkQueue(){return workQueue;}
    public static RedisConnectionPool getRedisPool(){return redisPool;}

    public static void initialize() {
        LOGGER.info("****************************Hello, darling !!!");
        LOGGER.info("Environment initialization is beginning...");

        try{
            if(ConfigurationHelper.WORK_MODEL.equals(Constant.WORK_MODEL_MASTER)){
                LOGGER.info("Work in master model!");




            }
            else if(ConfigurationHelper.WORK_MODEL.equals(Constant.WORK_MODEL_SLAVE)){
                LOGGER.info("Work in slave model!");
//                redisPool = new RedisConnectionPool(RedisInfo.AUDIENCE); TODO
                workQueue = new WorkQueue();
                AcceptCmd.getInstance().start();
                AudienceFileProcessor.getInstance().start();
            }
            else{
                LOGGER.error("Who am I? Please set work model!");
            }

        }
        catch (Exception ex){
            LOGGER.info("Environment initialization is error!!!", ex);
            dispose();
            return ;
        }
        LOGGER.info("Environment initialization completed successfully.");
    }

    public static void dispose(){
        LOGGER.info("Environment disposed is beginning...");

        if(ConfigurationHelper.WORK_MODEL.equals("master")){


        }
        else if(ConfigurationHelper.WORK_MODEL.equals("slave")){
            AcceptCmd.getInstance().stop();
            AudienceFileProcessor.getInstance().stop();
            if(null != redisPool){
                try {
                    redisPool.close();
                } catch (IOException e) {
                }
            }
        }

        LOGGER.info("Environment disposed.");
        LOGGER.info("****************************Bye, darling !!!");
    }

}
