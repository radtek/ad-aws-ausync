package com.upsmart.ausync.process.master;

import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.hang.netty.processor.HttpProcessor;
import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.model.AuQueryResponse;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.ActionType;
import com.upsmart.ausync.model.enums.TransCmd;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.GsonUtil;
import com.upsmart.server.common.utils.StringUtil;
import com.upsmart.server.trans.client.ClientProxy;
import com.upsmart.server.trans.client.ThriftClient;
import com.upsmart.server.trans.client.args.ThriftCliConnectionArgs;
import com.upsmart.server.trans.client.recvdata.RecvData;
import com.upsmart.server.trans.enums.RecvStatus;
import com.upsmart.server.trans.transinterface.BinaryData;
import com.upsmart.server.trans.transinterface.ClientInfo;
import com.upsmart.server.trans.transinterface.TransferInfo;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by yuhang on 17-9-28.
 *
 * redis接口查询
 */
public class AuQueryProcessor implements HttpProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuQueryProcessor.class);

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

        TransData ret = trans(transData);
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


    private TransData trans(TransData transData){
        if(null == transData){
            return null;
        }

        BinaryData bd = new BinaryData();
        bd.setData(transData.serializeJsonToGzip());

        ClientInfo ci = new ClientInfo();
        ci.setTime(DateUtil.format(new Date(), Constant.DATE_FORMAT_TOTAL));
        ci.setUsername("yuhang");

        TransferInfo ti = new TransferInfo();
        ti.setVersion(1);
        ti.setClientinfo(ci);
        ti.setData(bd);

        if(null != ti){
            // 传输配置
            int serverIndex = Constant.getRandomIndex(ConfigurationHelper.SLAVE_ADDRESSES);
            String slaveAdress = ConfigurationHelper.SLAVE_ADDRESSES.get(String.format("SLAVE_%d", serverIndex));
            ThriftCliConnectionArgs thriftArgs = new ThriftCliConnectionArgs(
                    slaveAdress,
                    ConfigurationHelper.SLAVE_LISTEN_PORT,
                    0);
            ClientProxy proxy = new ThriftClient();
            try {
                proxy.connect(thriftArgs);

                // 传输
                RecvData recvData;
                try{
                    recvData = proxy.query(TransCmd.AUDIENCE_UPDATE.getValue(), ti);
                }
                catch (Exception ex){
                    LOGGER.error("", ex);
                    return null;
                }
                if(null != recvData){
                    if(recvData.status.equals(RecvStatus.SUCCESS)){
                        if(null != recvData.data && null != recvData.data.data){
                            byte[] b = recvData.data.data.getData();
                            if(null != b) {
                                TransData retTransData = new TransData();
                                retTransData = (TransData) retTransData.deserializeFromGzip(b);
                                return retTransData;
                            }
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("", e);
            }
            finally {
                if(null != proxy){
                    try {
                        proxy.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        return null;
    }
}
