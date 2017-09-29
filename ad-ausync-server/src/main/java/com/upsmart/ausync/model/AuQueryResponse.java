package com.upsmart.ausync.model;

import java.util.List;

/**
 * Created by yuhang on 17-9-28.
 */
public class AuQueryResponse extends SerializeBase{

    public String resCode;
    public String errMsg;
    public List<TaskRes> taskRes;

    public class TaskRes{
        public String taskStat;
        public String taskId;
        public String errMsg;
    }
}
