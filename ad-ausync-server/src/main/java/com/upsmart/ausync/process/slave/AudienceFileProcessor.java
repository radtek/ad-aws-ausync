package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.awss3.AwsS3FileInfo;
import com.upsmart.ausync.awss3.AwsS3Wrapper;
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
                    LOGGER.warn(String.format("Ent to run task."));
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
            switch (at) {
                case UPDATE:
                    return update(task, localFiles);
                case DELETE:

                    break;
            }
            return false;
        }

        private boolean update(TransData.Task task, Map<String, BrotherFiles> map) throws Exception {
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
                setRedis(localFilePath);
            }

            return true;
        }

        private void gzipDecompress(String localFilePath) throws IOException {
            String cmd = String.format("gzip -dfk %s", localFilePath);
            BufferedReader input = null;
            try {
                Process process = Runtime.getRuntime().exec(cmd);
                if (null != process) {
                    input = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    if (null != input) {

                    }
                }
            } finally {
                if (null != input) {
                    try {
                        input.close();
                    } catch (IOException e) {
                    }
                }
            }
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

        private void setRedis(String localFilePath) throws IOException, InterruptedException {
            File file = new File(localFilePath);
            if (!file.isDirectory()) {
                LOGGER.info(String.format("set redis from (%s)", localFilePath));

                AudienceWrapper audienceWrapper = new AudienceWrapper(ConfigurationHelper.SLAVE_QUEUE_THREAD_COUNT);
                InputStreamReader inputStreamReader = null;
                BufferedReader bufferedReader = null;
                try {
                    inputStreamReader = new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-8");
                    bufferedReader = new BufferedReader(inputStreamReader);
                    String line;
                    List<String> deviceIds = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        if(null == deviceIds){
                            deviceIds = new ArrayList<>();
                        }
                        deviceIds.add(line);

                        if(deviceIds.size() >= ConfigurationHelper.SLAVE_QUEUE_BLOCK_SIZE){
                            audienceWrapper.offer(deviceIds);
                            deviceIds = null;
                        }
                    }
                    if(null != deviceIds && !deviceIds.isEmpty()) {
                        audienceWrapper.offer(deviceIds);
                    }
                    audienceWrapper.isWaiting();

                } finally {
                    if (null != bufferedReader) {
                        bufferedReader.close();
                    }
                    if (null != inputStreamReader) {
                        inputStreamReader.close();
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
                try {
                    in = new FileInputStream(file);
                    MappedByteBuffer byteBuffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
                    return MD5.encrypt(byteBuffer);
                } finally {
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
