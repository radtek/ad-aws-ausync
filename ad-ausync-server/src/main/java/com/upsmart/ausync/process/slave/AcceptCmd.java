package com.upsmart.ausync.process.slave;

import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.server.trans.server.ThriftService;
import com.upsmart.server.trans.server.args.ServConnectionArgs;
import com.upsmart.server.trans.server.args.ThriftServConnectionArgs;
import com.upsmart.server.trans.server.contract.Contract;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuhang on 17-5-17.
 *
 * 预算接收
 */
public class AcceptCmd {

    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptCmd.class);

    private static class Builder {
        public final static AcceptCmd instance = new AcceptCmd();
    }

    public static AcceptCmd getInstance() {
        return Builder.instance;
    }

    private AcceptCmd(){}

    private Thread thread;
    private ThriftService service;
    private boolean enable = true;

    public void start() {

        if(null == thread || !thread.isAlive()){
            Worker worker = new Worker();
            thread = new Thread(worker);
            thread.start();
        }
    }

    public void stop(){
        if(null != service){
            service.stop();
        }
        if(null != thread){
            thread.interrupt();
        }
        enable = false;
    }

    class Worker implements Runnable{

        @Override
        public void run() {

            while (enable){
                try {
                    service = new ThriftService();
                    Contract contract = new CmdContract();
                    ServConnectionArgs args = new ThriftServConnectionArgs(
                            ConfigurationHelper.SLAVE_LISTEN_PORT, contract, 8);

                    service.start(args);
                }
                catch (TTransportException te) {
                    LOGGER.error("",te);
                }
            }
            LOGGER.warn("AcceptCmd has been stopped!");
        }
    }
}
