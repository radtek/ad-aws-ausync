package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.awss3.AwsS3FileInfo;
import com.upsmart.ausync.awss3.AwsS3Wrapper;
import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.ausync.redis.AudienceWrapper;
import com.upsmart.server.common.codec.MD5;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.GZIPInputStream;

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

            while (enable) {
                try {
                    Thread.sleep(30000);

                    TransData.Task task = Environment.getWorkQueue().getNext();
                    if (null != task) {
                        LOGGER.warn(String.format("Begin to run task..."));

                        boolean success = false;
                        try {
                            if (process(task)) {
                                task.taskCode = "200";
                                Environment.getWorkQueue().updateStatus(task);
                                success = true;

                                LOGGER.warn(String.format("(%s) is successful", task.taskId));
                            }
                        } catch (Exception eex) {
                            LOGGER.error("", eex);
                        }
                        if (!success) {
                            task.retryNum--;
                            if (task.retryNum > 0) {
                                task.taskCode = "201";
                                Environment.getWorkQueue().updateStatus(task);
                                Environment.getWorkQueue().add(task);
                                LOGGER.warn(String.format("(%s) is error and retry.", task.taskId));
                            } else {
                                task.taskCode = "205";
                                Environment.getWorkQueue().updateStatus(task);
                                LOGGER.warn(String.format("(%s) is error.", task.taskId));
                            }
                        }

                        LOGGER.info(String.format("There are %d tasks to be run after a moment.", Environment.getWorkQueue().getCount()));
                    }
                } catch (InterruptedException iex) {
                    break;
                } catch (Exception ex) {
                    LOGGER.error("", ex);
                } finally {
                    LOGGER.info(String.format("End to run task."));
                }
            }

            LOGGER.warn("AudienceFileProcessor has been stopped!");
        }

        private boolean process(TransData.Task task) throws Exception {

            Map<String, BrotherFiles> localFiles = downloadFile(task);
            if (!VerifyMd5(localFiles)) {
                return false;
            }

            ActionType at = ActionType.convert(task.action);
            return update(task, localFiles, at);
        }

        private boolean update(TransData.Task task, Map<String, BrotherFiles> map, ActionType at) throws Exception {
            Iterator<Map.Entry<String, BrotherFiles>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, BrotherFiles> entry = iter.next();

                BrotherFiles brotherFiles = entry.getValue();
                if (!brotherFiles.verify) {
                    continue;
                }
                String localFilePath = brotherFiles.gzipFilePath.substring(0, brotherFiles.gzipFilePath.lastIndexOf("."));

                // 解压
                gzipDecompress(brotherFiles.gzipFilePath, localFilePath);
                // 读写redis
                setRedis(localFilePath, task.audienceIds, at);
            }

            return true;
        }

        private void gzipDecompress(String oldFile, String newFile) throws Exception {
            File file = new File(oldFile);
            FileInputStream fis = null;
            FileOutputStream fos = null;
            try{
                fis = new FileInputStream(file);
                fos = new FileOutputStream(newFile);
                decompress(fis, fos);
                fos.flush();
            }
            finally {
                if(null != fis) {
                    fis.close();
                }
                if(null != fos) {
                    fos.close();
                }
            }
        }

        public void decompress(InputStream is, OutputStream os) throws IOException {
            GZIPInputStream gis = null;
            try{
                gis = new GZIPInputStream(is);
                int count;
                byte data[] = new byte[1024];
                while ((count = gis.read(data, 0, 1024)) != -1) {
                    os.write(data, 0, count);
                }
            }
            finally {
                if(null != gis){
                    gis.close();
                }
            }
        }

        private void setRedis(String localFilePath, List<String> audienceIds, ActionType at) throws IOException, InterruptedException {
            File file = new File(localFilePath);
            if (!file.isDirectory()) {
                LOGGER.info(String.format("set redis from (%s)", localFilePath));

                AudienceWrapper audienceWrapper = new AudienceWrapper(ConfigurationHelper.SLAVE_QUEUE_THREAD_COUNT, audienceIds, at);

                FileInputStream in = null;
                FileChannel channel = null;
                List<MappedByteBuffer> listBuff = new ArrayList<>();
                try {

                    in = new FileInputStream(file);
                    channel = in.getChannel();

                    List<String> deviceIds = null;
                    boolean isNewLine = true;
                    int index = 0;
                    byte[] buff = null;
                    long fileLength = file.length();
                    long pro = fileLength/100; // 进度块
                    int proNum = 1;

                    // 文件块数量，可能还有结余
                    long blockNum = file.length() / Constant.MAX_MAPPING_BUFF_SIZE;
                    for(int m=0; m<blockNum; m++){
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, m*Constant.MAX_MAPPING_BUFF_SIZE, Constant.MAX_MAPPING_BUFF_SIZE);
                        listBuff.add(byteBuffer);

                        long bufferLength = byteBuffer.limit();
                        for(long i=0; i<bufferLength; i++){
                            if(isNewLine){
                                buff = new byte[1024];
                                index=0;
                                isNewLine = false;
                            }
                            byte b = byteBuffer.get();
                            if(b == '\n'){
                                String line = new String(Arrays.copyOf(buff, index));
                                if(null == deviceIds){
                                    deviceIds = new ArrayList<>();
                                }
                                deviceIds.add(line);
                                if(deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE){
                                    audienceWrapper.offer(deviceIds);
                                    deviceIds = null;
                                }
                                isNewLine = true;
                            }
                            else{
                                buff[index++] = b;
                            }

                            if((m * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                LOGGER.info(String.format("process %d: %d%%",m, (m * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                proNum++;
                            }
                        }
                    }
                    long remainByte = file.length() - (blockNum * Constant.MAX_MAPPING_BUFF_SIZE);
                    if(remainByte > 0){
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, (blockNum)*Constant.MAX_MAPPING_BUFF_SIZE, remainByte);
                        listBuff.add(byteBuffer);

                        long bufferLength = byteBuffer.limit();
                        for(long i=0; i<bufferLength; i++){
                            if(isNewLine){
                                buff = new byte[1024];
                                index=0;
                                isNewLine = false;
                            }
                            byte b = byteBuffer.get();
                            if(b == '\n'){
                                String line = new String(Arrays.copyOf(buff, index));
                                if(null == deviceIds){
                                    deviceIds = new ArrayList<>();
                                }
                                deviceIds.add(line);
                                if(deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE){
                                    audienceWrapper.offer(deviceIds);
                                    deviceIds = null;
                                }
                                isNewLine = true;
                            }
                            else{
                                buff[index++] = b;
                            }

                            if((blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) >= (pro * proNum)){
                                LOGGER.info(String.format("process r: %d%%", (blockNum * Constant.MAX_MAPPING_BUFF_SIZE + i) * 100/fileLength));
                                proNum++;
                            }
                        }
                    }

                    if(null != deviceIds && !deviceIds.isEmpty()) {
                        audienceWrapper.offer(deviceIds);
                    }
                    LOGGER.info("process: 100%");
                    audienceWrapper.isWaiting();
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

        private Map<String, BrotherFiles> downloadFile(TransData.Task task) {
            Map<String, BrotherFiles> map = new HashMap<>();

            AwsS3Wrapper awsS3Wrapper = new AwsS3Wrapper();
            String awsFolder = String.format("%s/%s", ConfigurationHelper.SLAVE_AWS_AUDIENCE_PATH, task.taskId);

            List<AwsS3FileInfo> list = awsS3Wrapper.getAllFilesPath(awsFolder);
            for (AwsS3FileInfo awsS3FileInfo : list) {
                String name = awsS3FileInfo.path.substring(awsS3FileInfo.path.lastIndexOf("/") + 1);
                String localFile = String.format("%s/%s/%s", ConfigurationHelper.SLAVE_LOCAL_AUDIENCE_PATH, task.taskId, name);
                File file = new File(localFile);
                awsS3Wrapper.downloadFile(awsS3FileInfo.path, file);
                LOGGER.warn(String.format("download (%s) => (%s)", awsS3FileInfo.path, localFile));

                String prefix = name.substring(0, name.indexOf("."));
                String suffix = name.substring(name.lastIndexOf("."));
                if (!map.containsKey(prefix)) {
                    map.put(prefix, new BrotherFiles());
                }
                if (suffix.equals(".md5")) {
                    map.get(prefix).md5FilePath = localFile;
                } else if (suffix.equals(".gz")) {
                    map.get(prefix).gzipFilePath = localFile;
                } else {
                    map.get(prefix).filePath = localFile;
                }
            }
            return map;
        }

        private boolean VerifyMd5(Map<String, BrotherFiles> map) throws IOException, NoSuchAlgorithmException {

            boolean ret = true;
            Iterator<Map.Entry<String, BrotherFiles>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, BrotherFiles> entry = iter.next();
                String prefix = entry.getKey();
                BrotherFiles brotherFiles = entry.getValue();

                String gzMD5 = readMD5(brotherFiles.gzipFilePath);
                String fileMD5 = readFileToStr(brotherFiles.md5FilePath);
                if (!StringUtil.isNullOrEmpty(gzMD5) && !StringUtil.isNullOrEmpty(fileMD5)) {
                    if (gzMD5.equals(fileMD5)) {
                        brotherFiles.verify = true;
                        LOGGER.info(String.format("(%s) MD5 is pass (%s).", prefix, gzMD5));
                    } else {
                        ret = false;
                        LOGGER.warn(String.format("(%s) MD5 is invalid (%s)-(%s).", prefix, gzMD5, fileMD5));
                    }
                } else {
                    brotherFiles.verify = true;
                    LOGGER.warn(String.format("(%s) no files to verify.", prefix));
                }
            }
            return ret;
        }

        private String readMD5(String filePath) throws NoSuchAlgorithmException, IOException {
            if (StringUtil.isNullOrEmpty(filePath)) {
                return null;
            }

            File file = new File(filePath);
            if (!file.isDirectory()) {
                FileInputStream in = null;
                FileChannel channel = null;
                List<MappedByteBuffer> listBuff = new ArrayList<>();
                try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    in = new FileInputStream(file);
                    channel = in.getChannel();

                    // 文件块数量，可能还有结余
                    long blockNum = file.length() / Constant.MAX_MAPPING_BUFF_SIZE;
                    for(int i=0; i<blockNum; i++){
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, i*Constant.MAX_MAPPING_BUFF_SIZE, Constant.MAX_MAPPING_BUFF_SIZE);
                        md.update(byteBuffer);
                        listBuff.add(byteBuffer);
                    }
                    long remainByte = file.length() - (blockNum * Constant.MAX_MAPPING_BUFF_SIZE);
                    if(remainByte > 0){
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, (blockNum)*Constant.MAX_MAPPING_BUFF_SIZE, remainByte);
                        md.update(byteBuffer);
                        listBuff.add(byteBuffer);
                    }

                    return MD5.byteToString(md.digest());
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
            return null;
        }


        private String readFileToStr(String filePath) throws IOException {
            if (StringUtil.isNullOrEmpty(filePath)) {
                return null;
            }

            String ret = "";
            File file = new File(filePath);
            if (!file.isDirectory()) {
                InputStreamReader inputStreamReader = null;
                BufferedReader bufferedReader = null;
                try {
                    inputStreamReader = new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-8");
                    bufferedReader = new BufferedReader(inputStreamReader);
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        ret += line;
                    }
                } finally {
                    if (null != bufferedReader) {
                        bufferedReader.close();
                    }
                    if (null != inputStreamReader) {
                        inputStreamReader.close();
                    }
                }
            }
            return ret;
        }
    }
}
