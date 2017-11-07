package com.upsmart.ausync.process.master;

import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.upsmart.ausync.model.AuUpdateRequest;
import com.upsmart.ausync.model.AuUpdateResponse;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.TransCmd;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.StringUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by yuhang on 17-11-6.
 */
public class TagUpdateProcessor extends BaseProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TagUpdateProcessor.class);

    @Override
    public boolean process(HttpRequestWrapper request, HttpResponseWrapper response) {
        AuUpdateRequest auUpdateRequest = new AuUpdateRequest();
        byte[] data = request.getBuff();
        if(null == data){
            return returnData(response, "600", "no body data in request", null);
        }
        else{
            try {
                String jsonStr = new String(data);
                auUpdateRequest = (AuUpdateRequest)auUpdateRequest.deserialize(jsonStr);
            }
            catch (Exception ex){
                LOGGER.error("", ex);
                return returnData(response, "600", "deserialize error", null);
            }

            if(null == auUpdateRequest){
                return returnData(response, "600", "fail to parse body data in request", null);
            }
            else{
                if(StringUtil.isNullOrEmpty(auUpdateRequest.taskId)){
                    return returnData(response, "600", "taskId is empty", null);
                }
            }
        }
        LOGGER.info("update >>> " + auUpdateRequest.serializeJson());

        TransData transData = new TransData();
        transData.tasks = new ArrayList<>();
        TransData.Task task = transData.new Task();
        task.action = auUpdateRequest.action;
        task.taskId = auUpdateRequest.taskId;
        task.time = Long.valueOf(DateUtil.format(new Date(), "yyyyMMddHHmmss"));
        transData.tasks.add(task);

        TransData ret = trans(transData, TransCmd.TAG_UPDATE.getValue());
        if(null == ret || null == ret.tasks || ret.tasks.isEmpty()){
            return returnData(response, "600", "fail to transfer data to remote service", null);
        }

        return returnData(response, "200", null, auUpdateRequest.taskId);
    }

    private boolean returnData(HttpResponseWrapper response, String resCode, String errMsg, String taskId){

        AuUpdateResponse auUpdateResponse = new AuUpdateResponse();
        auUpdateResponse.resCode = resCode;
        auUpdateResponse.errMsg = errMsg;
        auUpdateResponse.taskId = taskId;

        String resp = auUpdateResponse.serializeJson();

        response.setStringData(resp);
        response.setStatus(HttpResponseStatus.OK);
        return false;
    }
}
