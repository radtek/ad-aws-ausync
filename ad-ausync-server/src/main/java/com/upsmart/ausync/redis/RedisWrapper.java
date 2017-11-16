package com.upsmart.ausync.redis;

import com.upsmart.audienceproto.model.Audience;
import com.upsmart.audienceproto.serializer.AudienceSerializer;
import com.upsmart.ausync.model.AuRedis;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yuhang on 17-11-7.
 */
public abstract class RedisWrapper<T extends AuRedis> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(RedisWrapper.class);

    protected RedisConnectionPool redisCluster;
    /**
     * 数据队列
     */
    protected ConcurrentLinkedQueue<List<T>> queue;
    /**
     * 线程数量
     */
    protected int threadCount;

    /**
     * 数据输入完毕，用户等待结束
     */
    protected volatile boolean isCompleted;

    protected CountDownLatch countDownLatch;

    protected ActionType actionType;

    /**
     * 执行结果
     */
    protected boolean result = true;
    protected String resultStr = "";

    public RedisWrapper(int threadCount, ActionType at) throws URISyntaxException {
        this.redisCluster = new RedisConnectionPool(RedisInfo.AUDIENCE);
        this.threadCount = (threadCount < 1 ? 1 : threadCount);
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

    /**
     *
     * @param l
     * @throws InterruptedException
     */
    public void offer(List<T> l) throws InterruptedException {

        if(!result){
            throw new RuntimeException(resultStr);
        }

        while(queue.size() >= 10){
            Thread.sleep(100);
            continue;
        }
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

    protected abstract byte[] process(Audience au, T t);

    /**
     * 工作线程读写redis
     */
    private class Worker implements Runnable{

        private long count = 0;
        private long newCount = 0;

        @Override
        public void run() {

            while(true){
                try{
                    List<T> list = queue.poll();
                    if(null == list || list.isEmpty()){
                        if(isCompleted){
                            break; // 如果用户输入完毕，等待结束。此时如果队列没有数据，则退出循环结束线程
                        }
                        Thread.sleep(100);
                        continue;
                    }

                    for(T t : list){
                        Audience au = null;
                        if(!StringUtil.isNullOrEmpty(t.deviceId)){
                            byte[] key = t.deviceId.getBytes("UTF-8");
                            byte[] data = redisCluster.get(key);
                            if(null != data && data.length > 0) {
                                au = AudienceSerializer.parseFrom(data);
                            }

                            if(null == au && ActionType.UPDATE.equals(actionType)){
                                au = new Audience();
                                au.version = 1;
                                newCount++;
                            }

                            if(null != au) {
                                byte[] setData = process(au, t);
                                if(null != setData && setData.length > 0){
                                    redisCluster.set(key, setData); // redis.clients.jedis.exceptions.JedisDataException: OOM command not allowed when used memory > 'maxmemory'.
                                    count++;

                                    if( count % 10000 == 10 ){
                                        LOGGER.debug(String.format("sample device id:%s, %s",t.deviceId, au.toString()));
                                    }
                                }
                            }
                        }
                    }
                }
                catch (InterruptedException iex){
                    LOGGER.error("", iex);
                    break;
                }
                catch (Exception ex){
                    LOGGER.error(null, ex);
                    result = false;
                    resultStr = ex.getMessage();
                    break;
                }
                finally {

                }
            }
            LOGGER.debug(String.format("write (%d), and (%d) new count in it.", count, newCount));
            countDownLatch.countDown();
        }
    }
}
