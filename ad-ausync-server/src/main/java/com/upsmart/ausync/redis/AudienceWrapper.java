package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    public AudienceWrapper(int threadCount){
        this.redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);
        this.threadCount = (threadCount < 1 ? 1 : threadCount);
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
                    List<String> data = queue.poll();
                    if(null == data || data.isEmpty()){
                        if(isCompleted){
                            break; // 如果用户输入完毕，等待结束。此时如果队列没有数据，则退出循环结束线程
                        }
                        Thread.sleep(100);
                        continue;
                    }

                    for(String s : data){
                        count++;

                        // TODO

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
