package com.upsmart.ausync.core;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.process.slave.AcceptCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuhang on 17-5-15.
 */
public final class Environment {

    private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);


    public static void initialize(){
        LOGGER.info("****************************Hello, darling !!!");
        LOGGER.info("Environment initialization is beginning...");

        try{
            if(ConfigurationHelper.WORK_MODEL.equals(Constant.WORK_MODEL_MASTER)){
                LOGGER.info("Work in master model!");





            }
            else if(ConfigurationHelper.WORK_MODEL.equals(Constant.WORK_MODEL_SLAVE)){
                LOGGER.info("Work in slave model!");

                AcceptCmd.getInstance().start();
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

    public static void dispose()
    {
        LOGGER.info("Environment disposed is beginning...");

        if(ConfigurationHelper.WORK_MODEL.equals("master")){



        }
        else if(ConfigurationHelper.WORK_MODEL.equals("slave")){

            AcceptCmd.getInstance().stop();

        }

        LOGGER.info("Environment disposed.");
        LOGGER.info("****************************Bye, darling !!!");
    }

}
