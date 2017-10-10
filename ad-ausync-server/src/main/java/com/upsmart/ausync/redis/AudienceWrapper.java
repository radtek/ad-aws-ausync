package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yuhang on 17-10-10.
 */
public class AudienceWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceWrapper.class);

    private RedisConnectionPool redisCluster;
    /**
     * 数据队列
     */
    private ConcurrentLinkedQueue<List<String>> queue;
    /**
     * 线程数量
     */
    private int threadCount;

    /**
     * 数据输入完毕，用户等待结束
     */
    private volatile boolean isCompleted;

    private CountDownLatch countDownLatch;

    private List<String> audienceIds;

    private ActionType actionType;

    public AudienceWrapper(int threadCount, List<String> audienceIds, ActionType at){
        this.redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);
        this.threadCount = (threadCount < 1 ? 1 : threadCount);
        this.audienceIds = audienceIds;
        this.actionType = at;
        this.queue = new ConcurrentLinkedQueue<>();
        this.isCompleted = false;
        this.countDownLatch = new CountDownLatch(this.threadCount);
        for(int i=this.threadCount;i>0;i--){
            Worker worker = new Worker();
            Thread thread = new Thread(worker);
            thread.start();
        }
    }

    public void offer(List<String> l){
        queue.offer(l);
    }

    /**
     * 用户在输入数据完毕时调用
     * @throws InterruptedException
     */
    public void isWaiting() throws InterruptedException, IOException {
        isCompleted = true;
        if(null != countDownLatch) {
            countDownLatch.await();
        }
        if(null != redisCluster) {
            redisCluster.close();
        }
    }

    /**
     * 工作线程读写redis
     */
    private class Worker implements Runnable{

        private long count = 0;

        @Override
        public void run() {

            while(true){
                try{
                    List<String> list = queue.poll();
                    if(null == list || list.isEmpty()){
                        if(isCompleted){
                            break; // 如果用户输入完毕，等待结束。此时如果队列没有数据，则退出循环结束线程
                        }
                        Thread.sleep(100);
                        continue;
                    }

                    LOGGER.info(String.format("%d ", list.size()));

                    for(String deviceId : list){
                        count++;

                        Audience au = null;
                        if(!StringUtil.isNullOrEmpty(deviceId)){

                            byte[] key = deviceId.getBytes("UTF-8");
                            byte[] data = redisCluster.get(key);
                            if(null != data && data.length > 0) {
                                au = AudienceSerializer.parseFrom(data);
                            }

                            if(null == au && ActionType.UPDATE.equals(actionType)){
                                au = new Audience();
                                au.version = 1;
                                au.lastViewTime = DateUtil.dateToLong(new Date());
                            }

                            if(null == au.tags && ActionType.UPDATE.equals(actionType)){
                                au.tags = new HashSet<>();
                            }
                            if(ActionType.UPDATE.equals(actionType)) {
                                au.tags.addAll(audienceIds);
                            }
                            else {
                                au.tags.removeAll(audienceIds);
                            }

                            if(null != au){
                                byte[] b = AudienceSerializer.buildToBytes(au);
                                redisCluster.set(key,b);
                            }
                        }
                    }
                }
                catch (InterruptedException iex){
                    LOGGER.error("", iex);
                    break;
                }
                catch (Exception ex){
                    LOGGER.error("", ex);
                }
                finally {

                }
            }
            LOGGER.debug(String.format("write (%d)", count));
            countDownLatch.countDown();
        }
    }
}
