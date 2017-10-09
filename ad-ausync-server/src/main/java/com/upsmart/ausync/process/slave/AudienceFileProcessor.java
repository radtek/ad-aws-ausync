package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.awss3.AwsS3FileInfo;
import com.upsmart.ausync.awss3.AwsS3Wrapper;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.server.common.codec.MD5;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
                    if(null != task) {
                        LOGGER.warn(String.format("Begin to run task..."));

                        boolean success = false;
                        try{
                            if (process(task)) {
                                task.taskCode = "200";
                                Environment.getWorkQueue().updateStatus(task);
                                success = true;

                                LOGGER.warn(String.format("(%s) is successful", task.taskId));
                            }
                        }
                        catch (Exception eex){
                            LOGGER.error("", eex);
                            Environment.getWorkQueue().add(task);
                        }
                        if(!success){
                            task.retryNum--;
                            if(task.retryNum > 0){
                                task.taskCode = "201";
                                Environment.getWorkQueue().updateStatus(task);
                                Environment.getWorkQueue().add(task);
                                LOGGER.warn(String.format("(%s) is error and retry.", task.taskId));
                            }
                            else{
                                task.taskCode = "205";
                                Environment.getWorkQueue().updateStatus(task);
                                LOGGER.warn(String.format("(%s) is error.", task.taskId));
                            }
                        }

                        LOGGER.warn(String.format("Ent to run task."));
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

        private boolean process(TransData.Task task) throws IOException, NoSuchAlgorithmException {

            Map<String,BrotherFiles> localFiles = downloadFile(task);

            VerifyMd5(localFiles);

            ActionType at = ActionType.convert(task.action);
            switch (at){
                case UPDATE:

                    break;
                case DELETE:

                    break;
            }
            return false;
        }

        private Map<String,BrotherFiles> downloadFile(TransData.Task task){
            Map<String,BrotherFiles> map = new HashMap<>();

            AwsS3Wrapper awsS3Wrapper = new AwsS3Wrapper();
            String awsFolder = String.format("%s/%s", ConfigurationHelper.SLAVE_AWS_AUDIENCE_PATH, task.taskId);

            List<AwsS3FileInfo> list = awsS3Wrapper.getAllFilesPath(awsFolder);
            for(AwsS3FileInfo awsS3FileInfo : list){
                String name = awsS3FileInfo.path.substring(awsS3FileInfo.path.lastIndexOf("/")+1);
                String localFile = String.format("%s/%s/%s", ConfigurationHelper.SLAVE_LOCAL_AUDIENCE_PATH, task.taskId,name);
                File file = new File(localFile);
                awsS3Wrapper.downloadFile(awsS3FileInfo.path, file);
                LOGGER.warn(String.format("download (%s) => (%s)", awsS3FileInfo.path, localFile));

                String prefix = name.substring(0, name.indexOf("."));
                String suffix = name.substring(name.lastIndexOf("."));
                if(!map.containsKey(prefix)){
                    map.put(prefix, new BrotherFiles());
                }
                if(suffix.equals(".md5")){
                    map.get(prefix).md5FilePath = localFile;
                }
                else if(suffix.equals(".gz")){
                    map.get(prefix).gzipFilePath = localFile;
                }
                else{
                    map.get(prefix).filePath = localFile;
                }
            }
            return map;
        }

        private void VerifyMd5(Map<String,BrotherFiles> map) throws IOException, NoSuchAlgorithmException {

            Iterator<Map.Entry<String,BrotherFiles>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, BrotherFiles> entry = iter.next();
                String prefix = entry.getKey();
                BrotherFiles brotherFiles = entry.getValue();

                String gzMD5 = readMD5(brotherFiles.gzipFilePath);
                String fileMD5 = readFileToStr(brotherFiles.md5FilePath);
                if(!StringUtil.isNullOrEmpty(gzMD5) && !StringUtil.isNullOrEmpty(fileMD5)
                        && gzMD5.equals(fileMD5)){
                    brotherFiles.verify = true;
                    LOGGER.info(String.format("(%s) MD5 is pass (%s).", prefix, gzMD5));
                }
                else{
                    LOGGER.warn(String.format("(%s) MD5 is invalid (%s)-(%s).", prefix, gzMD5, fileMD5));
                }
            }
        }

        private String readMD5(String filePath) throws NoSuchAlgorithmException, IOException {
            File file = new File(filePath);
            if(!file.isDirectory()){
                FileInputStream in = null;
                try{
                    in = new FileInputStream(file);
                    MappedByteBuffer byteBuffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
                    return MD5.encrypt(byteBuffer);
                }
                finally{
                    if(null != in){
                        in.close();
                    }
                }
            }
            return null;
        }


        private String readFileToStr(String filePath) throws IOException {
            String ret = "";
            File file = new File(filePath);
            if(!file.isDirectory()){
                InputStreamReader inputStreamReader = null;
                BufferedReader bufferedReader = null;
                try{
                    inputStreamReader = new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-8");
                    bufferedReader = new BufferedReader(inputStreamReader);
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        ret += line;
                    }
                }
                finally{
                    if(null != bufferedReader){
                        bufferedReader.close();
                    }
                    if(null != inputStreamReader){
                        inputStreamReader.close();
                    }
                }
            }
            return ret;
        }
    }
}
