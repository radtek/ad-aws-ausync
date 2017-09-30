package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuhang on 17-9-30.
 */
public class AudienceFileProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceFileProcessor.class);

    private static class Builder {
        public final static AudienceFileProcessor instance = new AudienceFileProcessor();
    }
    public static AudienceFileProcessor getInstance() {
        return Builder.instance;
    }

    private AudienceFileProcessor() {
    }

    private Thread thread;
    private boolean enable = true;

    public void start() {

        if (null == thread || !thread.isAlive()) {
            Worker worker = new Worker();
            thread = new Thread(worker);
            thread.start();
        }
    }

    public void stop() {
        if (null != thread) {
            thread.interrupt();
        }
        enable = false;
        try {
            thread.join();
        } catch (InterruptedException e) {
        }
    }

    class Worker implements Runnable {
        @Override
        public void run() {

            while(enable){
                try{
                    Thread.sleep(1000);

                    TransData.Task task = Environment.getWorkQueue().getNext();
                    if(process(task)){
                        task.taskCode = "200";
                        Environment.getWorkQueue().updateStatus(task);
                    }
                    else{
                        task.taskCode = "201";
                        Environment.getWorkQueue().updateStatus(task);
                    }
                }
                catch (InterruptedException iex){
                    break;
                }
                catch(Exception ex){
                    LOGGER.error("", ex);
                }
                finally {

                }
            }

            LOGGER.warn("AudienceFileProcessor has been stopped!");
        }

        private boolean process(TransData.Task task){
            ActionType at = ActionType.convert(task.action);
            switch (at){
                case UPDATE:

                    break;
                case DELETE:

                    break;
            }
            return false;
        }
    }
}
