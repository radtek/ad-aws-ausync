package com.upsmart.ausync.process.slave;

import com.upsmart.audienceproto.model.TagScoreInfo;
import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.AuTagRedis;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.ausync.redis.AudienceWrapper;
import com.upsmart.ausync.redis.TagWrapper;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by yuhang on 17-11-7.
 */
public class TagFileProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TagFileProcessor.class);

    private static class Builder {
        public final static TagFileProcessor instance = new TagFileProcessor();
    }
    public static TagFileProcessor getInstance() {
        return Builder.instance;
    }

    private TagFileProcessor() {
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

                    task = Environment.getTagWorkQueue().getNext();
                    if (null != task) {
                        LOGGER.info(String.format("Begin to run TAG task..."));

                        boolean success = false;
                        try {
                            if (process(task)) {
                                task.taskCode = "200"; // 成功
                                task.taskMsg = "success";
                                Environment.getTagWorkQueue().updateStatus(task);
                                success = true;

                                LOGGER.info(String.format("(%s) is successful", task.taskId));
                            }
                        } catch (Exception eex) {
                            LOGGER.error("", eex);
                        }
                        if (!success) {
                            task.retryNum--;
                            if (task.retryNum > 0) {
                                Environment.getTagWorkQueue().updateStatus(task);
                                Environment.getTagWorkQueue().add(task);
                                LOGGER.warn(String.format("(%s) is error and retry.", task.taskId));
                            } else {
                                Environment.getTagWorkQueue().updateStatus(task);
                                LOGGER.warn(String.format("(%s) is error.", task.taskId));
                            }
                        }

                        LOGGER.info(String.format("There are %d tasks to be run after a moment.", Environment.getTagWorkQueue().getCount()));
                    }
                } catch (InterruptedException iex) {
                    break;
                } catch (Exception ex) {
                    LOGGER.error("", ex);
                } finally {
                    if(null != task) {
                        LOGGER.info(String.format("End to run TAG task."));
                    }
                }
            }

            LOGGER.warn("TagFileProcessor has been stopped!");
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
                // 由于HDFS直接传输S3无法MD5，所以去掉MD5验证
//                if (!brotherFiles.verify) {
//                    continue;
//                }
                if(null == brotherFiles.gzipFilePath){
                    continue;
                }

                String localFilePath = brotherFiles.gzipFilePath.substring(0, brotherFiles.gzipFilePath.lastIndexOf("."));

                // 解压
                gzipDecompress(brotherFiles.gzipFilePath, localFilePath);
                // 读写redis
                setRedis(localFilePath, at);
            }
            del(String.format("%s/%s", getLocalFilePath(), task.taskId));
            return true;
        }


        private void setRedis(String localFilePath, ActionType at) throws IOException, InterruptedException, URISyntaxException {
            File file = new File(localFilePath);
            if (!file.isDirectory()) {
                if(file.length() <= 0){
                    // 文件没有内容
                    LOGGER.warn(String.format("no content in (%s)", localFilePath));
                    return;
                }
                LOGGER.info(String.format("%s redis from (%s)",at.name(), localFilePath));

                TagWrapper audienceWrapper = new TagWrapper(ConfigurationHelper.SLAVE_QUEUE_THREAD_COUNT, at);

                FileInputStream in = null;
                FileChannel channel = null;
                List<MappedByteBuffer> listBuff = new ArrayList<>();
                try {

                    in = new FileInputStream(file);
                    channel = in.getChannel();

                    List<AuTagRedis> deviceIds = null;
                    boolean isNewLine = true;
                    int index = 0;
                    byte[] buff = null;
                    long fileLength = file.length();
                    long pro = fileLength/100; // 进度块
                    int proNum = 1;

                    // 文件块数量，可能还有结余
                    long blockNum = file.length() / Constant.MAX_MAPPING_BUFF_SIZE;
                    for(long m=0; m<blockNum; m++){
                        MappedByteBuffer byteBuffer = null;
                        try{
                            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, m*Constant.MAX_MAPPING_BUFF_SIZE, Constant.MAX_MAPPING_BUFF_SIZE);
//                            listBuff.add(byteBuffer);
                            long bufferLength = byteBuffer.limit();
                            for(long i=0; i<bufferLength; i++){
                                if(isNewLine){
                                    buff = new byte[4096];
                                    index=0;
                                    isNewLine = false;
                                }
                                byte b = byteBuffer.get();
                                if(b == '\n' || b == '\r'){
                                    if(index > 2) {
                                        String line = new String(Arrays.copyOf(buff, index));
                                        if (null == deviceIds) {
                                            deviceIds = new ArrayList<>();
                                        }
                                        deviceIds.addAll(parseLine(line));
                                        if (deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE) {
                                            audienceWrapper.offer(deviceIds);
                                            deviceIds = null;
                                        }
                                    }
                                    isNewLine = true;
                                }
                                else{
                                    if(index < 4096){
                                        buff[index++] = b;
                                    }
                                }

                                if((m * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                    LOGGER.info(String.format("process %d: %d%%",m, (m * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                    proNum++;
                                }
                            }
                        }
                        catch (Exception ex){
                            LOGGER.warn("", ex);
                        }
                        finally {
                            if(null != byteBuffer){
                                Constant.releaseBuff(byteBuffer); // 释放内存
                            }
                        }
                    }
                    long remainByte = file.length() - (blockNum * Constant.MAX_MAPPING_BUFF_SIZE);
                    if(remainByte > 0){
                        MappedByteBuffer byteBuffer=null;
                        try{
                            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, (blockNum)*Constant.MAX_MAPPING_BUFF_SIZE, remainByte);
//                            listBuff.add(byteBuffer);

                            long bufferLength = byteBuffer.limit();
                            for(long i=0; i<bufferLength; i++){
                                if(isNewLine){
                                    buff = new byte[4096];
                                    index=0;
                                    isNewLine = false;
                                }
                                byte b = byteBuffer.get();
                                if(b == '\n' || b == '\r'){
                                    if(index > 2) {
                                        String line = new String(Arrays.copyOf(buff, index));
                                        if (null == deviceIds) {
                                            deviceIds = new ArrayList<>();
                                        }
                                        deviceIds.addAll(parseLine(line));
                                        if (deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE) {
                                            audienceWrapper.offer(deviceIds);
                                            deviceIds = null;
                                        }
                                    }
                                    isNewLine = true;
                                }
                                else{
                                    if(index < 4096){
                                        buff[index++] = b;
                                    }
                                }

                                if((blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                    LOGGER.info(String.format("process r: %d%%", (blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                    proNum++;
                                }
                            }
                        }
                        catch (Exception ex){
                            LOGGER.warn("", ex);
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

        /**
         * 解析 每一行的数据: [device id1],[device id2]|tag1,value|tag2,value|....|tagN,value
         * @param line
         * @return
         */
        private List<AuTagRedis> parseLine(String line){
            if(StringUtil.isNullOrEmpty(line)){
                return null;
            }

            String[] arr = line.split(Constant.REGEX_SYMBOL_VERTICAL);
            if(null != arr && arr.length > 1){
                List<AuTagRedis> result = new ArrayList<>();
                try {
                    String[] devideIds = arr[0].split(Constant.SYMBOL_COMMA);
                    AuTagRedis auTagRedis = new AuTagRedis(devideIds[0]);
                    for (int i = 1; i < arr.length; ++i) {
                        if (null == arr[i]) {
                            continue;
                        }
                        String[] t = arr[i].split(Constant.SYMBOL_COMMA);
                        if (null != t && t.length >= 2) {
                            TagScoreInfo tsi = new TagScoreInfo();
                            tsi.tagId = t[0];
                            tsi.tagScore = Integer.valueOf(t[1]);
                            auTagRedis.tagScore.add(tsi);
                        }
                    }
                    result.add(auTagRedis);
                    // 处理多个id的状态
                    for(int i=1; i<devideIds.length; ++i){
                        AuTagRedis n = new AuTagRedis(auTagRedis, devideIds[i]);
                        result.add(n);
                    }
                    return result;
                }
                catch (Exception ex){
                    LOGGER.error(String.format("parse error (%s)", line), ex);
                }
            }
            return null;
        }

        @Override
        protected String getAwsS3FilePath() {
            return ConfigurationHelper.SLAVE_AWS_TAG_PATH;
        }

        @Override
        protected String getLocalFilePath() {
            return ConfigurationHelper.SLAVE_LOCAL_AUDIENCE_PATH + "/tag";
        }
    }
}
