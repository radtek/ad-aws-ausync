package com.upsmart.ausync.process.master;

import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.upsmart.ausync.model.AuQueryResponse;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.TransCmd;
import com.upsmart.server.common.utils.GsonUtil;
import com.upsmart.server.common.utils.StringUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuhang on 17-11-6.
 */
public class TagQueryProcessor extends BaseProcessor{

    private static final Logger LOGGER = LoggerFactory.getLogger(TagQueryProcessor.class);

    @Override
    public boolean process(HttpRequestWrapper request, HttpResponseWrapper response) {
        List<String> listData;
        byte[] data = request.getBuff();
        if(null == data){
            return returnData(response, "600", "no body data in request", null);
        }
        else{
            try {
                String jsonStr = new String(data);
                GsonUtil gson = new GsonUtil();
                listData = gson.deserialize(jsonStr, List.class);
            }
            catch (Exception ex){
                LOGGER.error("", ex);
                return returnData(response, "600", "deserialize error", null);
            }

            if(null == listData || listData.isEmpty()){
                return returnData(response, "600", "fail to parse body data in request", null);
            }
        }

        LOGGER.info("query >>> " + StringUtil.listToString(listData));

        TransData transData = new TransData();
        transData.tasks = new ArrayList<>();
        for(String s : listData){
            TransData.Task task = transData.new Task();
            task.taskId = s;
            transData.tasks.add(task);
        }

        TransData ret = trans(transData, TransCmd.TAG_QUERY.getValue());
        if(null == ret || null == ret.tasks || ret.tasks.isEmpty()){
            return returnData(response, "600", "fail to transfer data to remote service", null);
        }

        return returnData(response, "200", null, ret.tasks);
    }

    private boolean returnData(HttpResponseWrapper response, String resCode, String errMsg, List<TransData.Task> taskRes){

        AuQueryResponse auQueryResponse = new AuQueryResponse();
        auQueryResponse.resCode = resCode;
        auQueryResponse.errMsg = errMsg;
        if(null != taskRes) {
            auQueryResponse.taskRes = new ArrayList<>();
            for (TransData.Task task : taskRes) {
                AuQueryResponse.TaskRes t = auQueryResponse.new TaskRes();
                t.taskStat = task.taskCode;
                t.taskId = task.taskId;
                t.errMsg = task.taskMsg;
                auQueryResponse.taskRes.add(t);
            }
        }

        String resp = auQueryResponse.serializeJson();

        response.setStringData(resp);
        response.setStatus(HttpResponseStatus.OK);
        return false;
    }
}
