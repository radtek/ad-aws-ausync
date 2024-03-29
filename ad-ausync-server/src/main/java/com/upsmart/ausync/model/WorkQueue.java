package com.upsmart.ausync.model;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by yuhang on 17-9-29.
 */
public class WorkQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkQueue.class);

    private String historyFilePath;
    private ConcurrentLinkedDeque<TransData.Task> workQueue;
    private ConcurrentHashMap<String, TransData.Task> statusMap;
    private CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);

    public WorkQueue(String historyFilePath) throws IOException {
        this.historyFilePath = historyFilePath;
        workQueue = new ConcurrentLinkedDeque<>();
        statusMap = new ConcurrentHashMap<>();

        Path dirPath = Paths.get(historyFilePath);
        Files.createDirectories(dirPath);
        readFile(historyFilePath);
    }

    public void add(TransData transData) throws IOException {
        if(null == transData || null == transData.tasks){
            return;
        }
        for(TransData.Task task : transData.tasks){
            add(task);
        }
    }
    public void add(TransData.Task task) throws IOException {
        if(statusMap.containsKey(task.taskId)){
            TransData.Task t = statusMap.get(task.taskId);
            if(null != t.taskCode && t.taskCode.equals("200")){
                LOGGER.warn(String.format("task(%s) is exist 200.", task.taskId));
            }
        }
        statusMap.put(task.taskId, task);
        workQueue.offer(task);

        StringBuilder sb = new StringBuilder();
        sb.append(new Date()).append(Constant.SYMBOL_VERTICAL);
        sb.append(task.serializeJson());
        sb.append(Constant.LINE_SEPARATOR);
        writeFile(historyFilePath, task.taskId, sb.toString());
    }

    public TransData.Task getNext(){
        return workQueue.poll();
    }

    public int getCount(){
        return workQueue.size();
    }

    public void updateStatus(TransData.Task task) throws IOException {
        TransData.Task t = statusMap.get(task.taskId);
        if(null != t){
            t.taskCode = task.taskCode;
            t.taskMsg = task.taskMsg;

            StringBuilder sb = new StringBuilder();
            sb.append(new Date()).append(Constant.SYMBOL_VERTICAL);
            sb.append(t.serializeJson());
            sb.append(Constant.LINE_SEPARATOR);
            writeFile(historyFilePath, t.taskId, sb.toString());
        }
    }

    public TransData getStatus(TransData query){
        TransData ret = new TransData();
        ret.tasks = new ArrayList<>();
        if(null == query || null == query.tasks || query.tasks.isEmpty()){
            return ret;
        }

        for(TransData.Task task : query.tasks){
            if(StringUtil.isNullOrEmpty(task.taskId)){
                continue;
            }

            TransData.Task t = statusMap.get(task.taskId);
            if(null != t){
                ret.tasks.add(t);
            }
        }

        return ret;
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
                                if(null == statusMap){
                                    statusMap = new ConcurrentHashMap<>();
                                }
                                if(null != task.taskCode
                                        && !task.taskCode.equals("200")){
                                    task.taskCode = "207";
                                    task.taskMsg = "application restarted before task finished";
                                }
                                statusMap.put(task.taskId, task);
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

        LOGGER.info(String.format("read %d history tasks.", statusMap.size()));
    }

    private void writeFile(String dirName,String taskId, String taskStr) throws IOException {
        Path filePath = Paths.get(dirName, "audience-task-"+taskId);

        File f = new File(filePath.toString());
        if(f.isFile() && f.exists()) {
            f.delete();
        }

        OutputStream writer = null;
        try{
            writer = new BufferedOutputStream(
                    Files.newOutputStream(filePath, StandardOpenOption.CREATE),
                    131072); // 128k

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

    /**
     * 根据audience id 获得最近的一次更新audience的taskid
     * @param newTask
     * @return
     */
    public TransData.Task getLatestTaskId(TransData.Task newTask){
        if(null == newTask || null == newTask.audienceIds || newTask.audienceIds.isEmpty()){
            return null;
        }

        TransData.Task result = null;
        long time = -1;
        Iterator<Map.Entry<String, TransData.Task>> iter = statusMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, TransData.Task> entry = iter.next();
            TransData.Task tt = entry.getValue();

            if(null == tt.audienceIds || tt.audienceIds.isEmpty()){
                continue;
            }
            String aid = tt.audienceIds.get(0);
            if(aid.equals(newTask.audienceIds.get(0)) && tt.taskCode.equals("200") && tt.time > time){
                result = tt;
                time = tt.time;
            }
        }

        return result;
    }
}
