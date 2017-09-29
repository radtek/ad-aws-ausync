package com.upsmart.ausync.model;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.server.common.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by yuhang on 17-9-29.
 */
public class WorkQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkQueue.class);

    private ConcurrentLinkedDeque<TransData.Task> workQueue;
    private ConcurrentHashMap<String, TransData.Task> stateMap;
    private CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);

    public WorkQueue() throws IOException {
        workQueue = new ConcurrentLinkedDeque<>();
        stateMap = new ConcurrentHashMap<>();

        Path dirPath = Paths.get(ConfigurationHelper.SLAVE_HISTORY_LOG);
        Files.createDirectories(dirPath);
        readFile(ConfigurationHelper.SLAVE_HISTORY_LOG);
    }

    public void add(TransData transData) throws IOException {
        if(null == transData || null == transData.tasks){
            return;
        }
        for(TransData.Task task : transData.tasks){

            if(stateMap.containsKey(task.taskId)){
                TransData.Task t = stateMap.get(task.taskId);
                if(null != t.taskCode && t.taskCode.equals("200")){
                    LOGGER.warn(String.format("task(%s) is exist 200.", task.taskId));
                }
            }
            stateMap.put(task.taskId, task);
            workQueue.offer(task);

            StringBuilder sb = new StringBuilder();
            sb.append(new Date()).append(Constant.SYMBOL_VERTICAL);
            sb.append(task.serializeJson());
            sb.append(Constant.LINE_SEPARATOR);
            writeFile(ConfigurationHelper.SLAVE_HISTORY_LOG, sb.toString());
        }
    }

    public TransData.Task getNext(){
        return workQueue.poll();
    }

    public void updateState(TransData.Task task){
        TransData.Task t = stateMap.get(task.taskId);
        if(null != t){
            t.taskCode = task.taskCode;
            t.taskMsg = task.taskMsg;
        }
    }

    private void readFile(String dirName) throws IOException {

        File root = new File(dirName);
        File[] files = root.listFiles();
        if(null == files || files.length == 0){
            return;
        }
        LOGGER.info(String.format("read %d history files.", files.length));

        for(File file : files){
            if(!file.isDirectory()){
                InputStreamReader inputStreamReader = null;
                BufferedReader bufferedReader = null;
                try{
                    TransData transData = new TransData();
                    inputStreamReader = new InputStreamReader(new FileInputStream(file.getAbsolutePath()), "UTF-8");
                    bufferedReader = new BufferedReader(inputStreamReader);
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] arr = line.split(Constant.REGEX_SYMBOL_VERTICAL);
                        if(null != arr && arr.length >=2) {
                            TransData.Task task = transData.new Task();
                            task = (TransData.Task)task.deserialize(arr[1]);
                            if(null != task){
                                if(null == stateMap){
                                    stateMap = new ConcurrentHashMap<>();
                                }
                                stateMap.put(task.taskId, task);
                            }
                        }
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
        }

        LOGGER.info(String.format("read %d history tasks.", stateMap.size()));
    }

    private void writeFile(String dirName, String taskStr) throws IOException {
        Path filePath = Paths.get(dirName, DateUtil.format(new Date(),"yyyyMMdd"));
        OutputStream writer = null;
        try{
            writer = new BufferedOutputStream(
                    Files.newOutputStream(filePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE),
                    262144); // 256k

            if (writer != null) {
                ByteBuffer bbuf = encoder.encode(CharBuffer.wrap(taskStr));
                writer.write(bbuf.array(), bbuf.arrayOffset(), bbuf.remaining());
            }
        }
        finally {
            if(null != writer){
                writer.close();
            }
        }
    }
}
