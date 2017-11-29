package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.awss3.AwsS3FileInfo;
import com.upsmart.ausync.awss3.AwsS3Wrapper;
import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.ausync.model.TransData;
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
 * Created by yuhang on 17-11-7.
 */
public abstract class FileProcessor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);

    /**
     * 文在在aws的s3路径
     */
    protected abstract String getAwsS3FilePath();
    /**
     * 文件下载到本地的路径
     */
    protected abstract String getLocalFilePath();

    protected Map<String, BrotherFiles> downloadFile(TransData.Task task) {
        if(null == task){
            return null;
        }

        Map<String, BrotherFiles> map = new HashMap<>();

        AwsS3Wrapper awsS3Wrapper = new AwsS3Wrapper();
        String awsFolder = String.format("%s/%s", getAwsS3FilePath(), task.taskId);

        List<AwsS3FileInfo> list = awsS3Wrapper.getAllFilesPath(awsFolder);
        for (AwsS3FileInfo awsS3FileInfo : list) {
            String name = awsS3FileInfo.path.substring(awsS3FileInfo.path.lastIndexOf("/") + 1);
            String localFile = String.format("%s/%s/%s", getLocalFilePath(), task.taskId, name);
            File file = new File(localFile);
            awsS3Wrapper.downloadFile(awsS3FileInfo.path, file);
            LOGGER.warn(String.format("download (%s) => (%s)", awsS3FileInfo.path, localFile));

            String prefix = name.substring(0, name.indexOf("."));
            String suffix = name.substring(name.lastIndexOf("."));

            if (suffix.equals(".md5")) {
                if (!map.containsKey(prefix)) {
                    map.put(prefix, new BrotherFiles());
                }
                map.get(prefix).md5FilePath = localFile;
            } else if (suffix.equals(".gz")) {
                if (!map.containsKey(prefix)) {
                    map.put(prefix, new BrotherFiles());
                }
                map.get(prefix).gzipFilePath = localFile;
            } else {
                // 其它后缀的文件
//                    map.get(prefix).filePath = localFile;
            }
        }
        return map;
    }

    protected void gzipDecompress(String oldFile, String newFile) throws Exception {
        LOGGER.info(String.format("decompress (%s) to (%s)", oldFile, newFile));

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

    protected void decompress(InputStream is, OutputStream os) throws IOException {
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

    protected boolean del(String filePath){
        File f = new File(filePath);
        if(f.isDirectory()){
            for(File s : f.listFiles()){
                del(s.getAbsolutePath());
            }
        }
        boolean delStatus = f.delete();
        LOGGER.info(String.format("delete (%s)", filePath, delStatus));
        return delStatus;
    }

    protected boolean VerifyMd5(Map<String, BrotherFiles> map) throws IOException, NoSuchAlgorithmException {

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


    protected String readMD5(String filePath) throws NoSuchAlgorithmException, IOException {
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
                    MappedByteBuffer byteBuffer = null;
                    try{
                        byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, i*Constant.MAX_MAPPING_BUFF_SIZE, Constant.MAX_MAPPING_BUFF_SIZE);
                        md.update(byteBuffer);
//                      listBuff.add(byteBuffer);
                    }
                    finally {
                        if(null != byteBuffer){
                            Constant.releaseBuff(byteBuffer); // 释放内存
                        }
                    }
                }
                long remainByte = file.length() - (blockNum * Constant.MAX_MAPPING_BUFF_SIZE);
                if(remainByte > 0){
                    MappedByteBuffer byteBuffer = null;
                    try{
                        byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, (blockNum)*Constant.MAX_MAPPING_BUFF_SIZE, remainByte);
                        md.update(byteBuffer);
//                      listBuff.add(byteBuffer);
                    }
                    finally {
                        if(null != byteBuffer){
                            Constant.releaseBuff(byteBuffer); // 释放内存
                        }
                    }
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


    protected String readFileToStr(String filePath) throws IOException {
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
