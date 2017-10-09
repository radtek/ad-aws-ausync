package com.upsmart.ausync.model;

import java.util.List;

/**
 * Created by yuhang on 17-9-29.
 */
public class TransData extends SerializeBase{

    public List<Task> tasks;

    public class Task extends SerializeBase{
        public String taskId;
        public String action;
        public List<String> audienceIds;
        public String taskCode = "0";
        public String taskMsg = "";
        public int retryNum = 10; // 重试次数
    }
}
