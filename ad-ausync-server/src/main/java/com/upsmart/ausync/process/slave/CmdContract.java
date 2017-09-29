package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.common.Constant;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.model.TransData;
import com.upsmart.ausync.model.enums.TransCmd;
import com.upsmart.server.common.utils.DateUtil;
import com.upsmart.server.common.utils.StringUtil;
import com.upsmart.server.trans.server.contract.Contract;
import com.upsmart.server.trans.transinterface.BinaryData;
import com.upsmart.server.trans.transinterface.ClientInfo;
import com.upsmart.server.trans.transinterface.TransferInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Created by yuhang on 17-5-17.
 */
public class CmdContract implements Contract {

    private static final Logger LOGGER = LoggerFactory.getLogger(CmdContract.class);

    @Override
    public boolean ping() throws TException {
        return true;
    }

    @Override
    public TransferInfo query(String cmd, TransferInfo trans) throws TException {

        TransCmd transCmd = TransCmd.convert(cmd);
        printTrans(transCmd, trans);
        switch (transCmd){
            case AUDIENCE_UPDATE: // 更新max
                LOGGER.info(String.format(">>> Begin to audience update."));

                byte[] data = getData(trans);
                TransData task = new TransData();
                task = (TransData)task.deserializeFromGzip(data);
                try {
                    Environment.getWorkQueue().add(task);
                } catch (IOException e) {
                    LOGGER.info("", e);
                }
                LOGGER.info(String.format("<<< End to audience update."));
                break;

            case AUDIENCE_QUERY:
                LOGGER.info(String.format(">>> Begin to audience query."));






                LOGGER.info(String.format("<<< End to audience query."));
                break;

            case UNKNOWN:
                LOGGER.warn(String.format(">>> Unknown command.<<<"));
                break;
        }
        return response(2);
    }

    private void printTrans(TransCmd transCmd, TransferInfo trans){
        if(null == trans){
            LOGGER.warn(String.format("%s TransferInfo is null!!!", transCmd.name()));
            return;
        }
        ClientInfo ci = trans.getClientinfo();
        if(null == ci){
            LOGGER.warn(String.format("%s ClientInfo is null!!!", transCmd.name()));
            return;
        }
        String t = ci.getTime();
        if(!StringUtil.isNullOrEmpty(t)){
            LOGGER.info(String.format("%s a new TransferInfo accepted: %s", transCmd.name(), t));
        }
    }

    private byte[] getData(TransferInfo trans){
        if(null == trans){
            return null;
        }
        BinaryData bd = trans.getData();
        if(null == bd){
            return null;
        }
        byte[] bytes = bd.getData();
        if(null == bytes || 0 >= bytes.length){
            return null;
        }
        return bytes;
    }

    private TransferInfo response(long status){
        return response(status, null);
    }

    private TransferInfo response(long status, byte[] bytes){

        TransferInfo ret = new TransferInfo();
        ret.setVersion(1);
        ret.setClientinfo(getClientInfo());
        ret.setStatus(status);
        if(null != bytes) {
            BinaryData bd = new BinaryData();
            bd.setData(bytes);
            ret.setData(bd);
        }
        return ret;
    }

    private ClientInfo getClientInfo(){
        ClientInfo ci = new ClientInfo();
        ci.setTime(DateUtil.format(new Date(), Constant.DATE_FORMAT_TOTAL));
        ci.setUsername("yuhang");
        return ci;
    }
}
