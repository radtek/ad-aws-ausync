package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.AuRedis;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.ausync.redis.AudienceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

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

    class Worker extends FileProcessor {
        @Override
        public void run() {

            while (enable) {
                TransData.Task task = null;
                try {
                    Thread.sleep(30000);

                    task = Environment.getAudienceWorkQueue().getNext();
                    if (null != task) {
                        LOGGER.info(String.format("Begin to run AUDIENCE task..."));

                        boolean success = false;
                        try {
                            if (process(task)) {
                                task.taskCode = "200"; // 成功
                                task.taskMsg = "success";
                                Environment.getAudienceWorkQueue().updateStatus(task);
                                success = true;

                                LOGGER.info(String.format("(%s) is successful", task.taskId));
                            }
                        } catch (Exception eex) {
                            LOGGER.error("", eex);
                        }
                        if (!success) {
                            task.retryNum--;
                            if (task.retryNum > 0) {
                                Environment.getAudienceWorkQueue().updateStatus(task);
                                Environment.getAudienceWorkQueue().add(task);
                                LOGGER.warn(String.format("(%s) is error and retry.", task.taskId));
                            } else {
                                Environment.getAudienceWorkQueue().updateStatus(task);
                                LOGGER.warn(String.format("(%s) is error.", task.taskId));
                            }
                        }

                        LOGGER.info(String.format("There are %d tasks to be run after a moment.", Environment.getAudienceWorkQueue().getCount()));
                    }
                } catch (InterruptedException iex) {
                    break;
                } catch (Exception ex) {
                    LOGGER.error("", ex);
                } finally {
                    if(null != task) {
                        LOGGER.info(String.format("End to run AUDIENCE task."));
                    }
                }
            }

            LOGGER.warn("AudienceFileProcessor has been stopped!");
        }

        private boolean process(TransData.Task task) throws Exception {
            task.taskCode = "201"; // 正在进行

            Map<String, BrotherFiles> localFiles = downloadFile(task);
            if(null == localFiles || localFiles.isEmpty()){
                task.taskCode = "204";
                task.taskMsg = "files in S3 not found";
                return false;
            }

            if (!VerifyMd5(localFiles)) {
                task.taskCode = "205";
                task.taskMsg = "Verify MD5 error";
                return false;
            }

            ActionType at = ActionType.convert(task.action);
            try{
                if(task.action.equals(ActionType.UPDATE.getValue())){
                    // 获得当前audience的上一次更新
                    TransData.Task latestTask = Environment.getAudienceWorkQueue().getLatestTaskId(task);
                    if(null != latestTask) {
                        LOGGER.info(String.format("DELETE data by latest taskid(%s)", latestTask.taskId));
                        Map<String, BrotherFiles> latestTaskFiles = downloadFile(latestTask);
                        if(null != latestTaskFiles && VerifyMd5(latestTaskFiles)) {
                            update(latestTask, latestTaskFiles, ActionType.DELETE);
                        }
                    }
                }
                return update(task, localFiles, at);
            }
            catch (Exception ex){
                task.taskCode = "206";
                task.taskMsg = ex.getMessage();

                LOGGER.error("", ex);
            }
            return false;
        }

        private boolean update(TransData.Task task, Map<String, BrotherFiles> map, ActionType at) throws Exception {
            if(null == task || null == map){
                return false;
            }
            Iterator<Map.Entry<String, BrotherFiles>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, BrotherFiles> entry = iter.next();

                BrotherFiles brotherFiles = entry.getValue();
                if (!brotherFiles.verify) {
                    continue;
                }
                if(null == brotherFiles.gzipFilePath){
                    continue;
                }

                String localFilePath = brotherFiles.gzipFilePath.substring(0, brotherFiles.gzipFilePath.lastIndexOf("."));

                // 解压
                gzipDecompress(brotherFiles.gzipFilePath, localFilePath);
                // 读写redis
                setRedis(localFilePath, task.audienceIds, at);
            }
            del(String.format("%s/%s", getLocalFilePath(), task.taskId));
            return true;
        }

        private void setRedis(String localFilePath, List<String> audienceIds, ActionType at) throws IOException, InterruptedException, URISyntaxException {
            File file = new File(localFilePath);
            if (!file.isDirectory()) {
                if(file.length() <= 0){
                    // 文件没有内容
                    LOGGER.warn(String.format("no content in (%s)", localFilePath));
                    return;
                }
                LOGGER.info(String.format("%s redis from (%s)",at.name(), localFilePath));

                AudienceWrapper audienceWrapper = new AudienceWrapper(ConfigurationHelper.SLAVE_QUEUE_THREAD_COUNT, audienceIds, at);

                FileInputStream in = null;
                FileChannel channel = null;
                List<MappedByteBuffer> listBuff = new ArrayList<>();
                try {

                    in = new FileInputStream(file);
                    channel = in.getChannel();

                    List<AuRedis> deviceIds = null;
                    boolean isNewLine = true;
                    int index = 0;
                    byte[] buff = null;
                    long fileLength = file.length();
                    long pro = fileLength/100; // 进度块
                    int proNum = 1;

                    // 文件块数量，可能还有结余
                    long blockNum = file.length() / Constant.MAX_MAPPING_BUFF_SIZE;
                    for(int m=0; m<blockNum; m++){
                        MappedByteBuffer byteBuffer = null;
                        try{
                            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, m*Constant.MAX_MAPPING_BUFF_SIZE, Constant.MAX_MAPPING_BUFF_SIZE);
//                            listBuff.add(byteBuffer);

                            long bufferLength = byteBuffer.limit();
                            for(long i=0; i<bufferLength; i++){
                                if(isNewLine){
                                    buff = new byte[1024];
                                    index=0;
                                    isNewLine = false;
                                }
                                byte b = byteBuffer.get();
                                if(b == '\n' || b == '\r'){
                                    if(index >2) {
                                        String line = new String(Arrays.copyOf(buff, index));
                                        if (null == deviceIds) {
                                            deviceIds = new ArrayList<>();
                                        }
                                        deviceIds.add(new AuRedis(line));
                                        if (deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE) {
                                            audienceWrapper.offer(deviceIds);
                                            deviceIds = null;
                                        }
                                    }
                                    isNewLine = true;
                                }
                                else{
                                    if(index < 1024){
                                        buff[index++] = b;
                                    }
                                }

                                if((m * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                    LOGGER.info(String.format("process %d: %d%%",m, (m * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                    proNum++;
                                }
                            }
                        }
                        finally {
                            if(null != byteBuffer) {
                                Constant.releaseBuff(byteBuffer); // 释放内存
                            }
                        }
                    }
                    long remainByte = file.length() - (blockNum * Constant.MAX_MAPPING_BUFF_SIZE);
                    if(remainByte > 0){
                        MappedByteBuffer byteBuffer = null;
                        try{
                            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, (blockNum)*Constant.MAX_MAPPING_BUFF_SIZE, remainByte);
//                            listBuff.add(byteBuffer);

                            long bufferLength = byteBuffer.limit();
                            for(long i=0; i<bufferLength; i++){
                                if(isNewLine){
                                    buff = new byte[1024];
                                    index=0;
                                    isNewLine = false;
                                }
                                byte b = byteBuffer.get();
                                if(b == '\n' || b == '\r'){
                                    if(index >2) {
                                        String line = new String(Arrays.copyOf(buff, index));
                                        if (null == deviceIds) {
                                            deviceIds = new ArrayList<>();
                                        }
                                        deviceIds.add(new AuRedis(line));
                                        if (deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE) {
                                            audienceWrapper.offer(deviceIds);
                                            deviceIds = null;
                                        }
                                    }
                                    isNewLine = true;
                                }
                                else{
                                    if(index < 1024){
                                        buff[index++] = b;
                                    }
                                }

                                if((blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                    LOGGER.info(String.format("process r: %d%%", (blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                    proNum++;
                                }
                            }
                        }
                        finally {
                            if(null != byteBuffer){
                                Constant.releaseBuff(byteBuffer); // 释放内存
                            }
                        }
                    }

                    if(null != deviceIds && !deviceIds.isEmpty()) {
                        audienceWrapper.offer(deviceIds);
                    }
                    audienceWrapper.isWaiting();
                    LOGGER.info("process: 100%");
                } finally {
                    if(null != listBuff && !listBuff.isEmpty()){
                        for(MappedByteBuffer buff : listBuff){
                            Constant.releaseBuff(buff); // 释放内存
                        }
                    }
                    if(null != channel){
                        channel.close();
                    }
                    if (null != in) {
                        in.close();
                    }
                }
            }
        }

        @Override
        protected String getAwsS3FilePath() {
            return ConfigurationHelper.SLAVE_AWS_AUDIENCE_PATH;
        }

        @Override
        protected String getLocalFilePath() {
            return ConfigurationHelper.SLAVE_LOCAL_AUDIENCE_PATH + "/audience";
        }
    }
}
