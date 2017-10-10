package com.upsmart;

import com.hang.netty.HttpServer;
import com.hang.netty.processor.DefaultProcessor;
import com.hang.netty.model.ServerConfig;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.core.Environment;
import com.upsmart.ausync.process.master.AuQueryProcessor;
import com.upsmart.ausync.process.master.AuUpdateProcessor;
import com.upsmart.ausync.process.slave.AuTestProcessor;
import com.upsmart.server.configuration.ConfigurationManager;
import org.apache.log4j.PropertyConfigurator;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Created by upsmart on 17-8-3.
 */
public class ServiceStart {

    public static void main(String[] args)  throws Exception {
        final HttpServer srv = new HttpServer();
        ServerConfig conf = new ServerConfig();

        // 优雅关闭
        SignalHandler handler = new SignalHandler() {
            public void handle(Signal signal) {
                Environment.dispose();
                srv.stop();
            }
        };

        Signal.handle(new Signal("TERM"), handler);
        Signal.handle(new Signal("INT"), handler);
        Signal.handle(new Signal("USR2"), handler);

        // 外部参数接收并初始化
        if(args != null && args.length > 0){
            String path = args[0];

            PropertyConfigurator.configure(path + "log4j.properties");
            ConfigurationManager.initInstance(path + "web.config");
            Environment.initialize();
        }
        else{
            throw new RuntimeException("Application args inputted was invalid!");
        }

        conf.addProcessor("/cjdttg", new DefaultProcessor());
        conf.addProcessor("/audience/pushtrans", new AuUpdateProcessor());
        conf.addProcessor("/audience/query", new AuQueryProcessor());
        conf.addProcessor("/audience/test", new AuTestProcessor());
        conf.setHttpPort(ConfigurationHelper.HTTP_PORT);

        // 服务启动
        srv.start(conf);
        System.exit(0);
    }
}
