package com.upsmart.ausync.redis;

import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.server.common.utils.StringUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yuhang on 17-10-9.
 */
public enum RedisInfo {

    AUDIENCE(ConfigurationHelper.SLAVE_REDIS.get("IP"),
            ConfigurationHelper.SLAVE_REDIS.get("PORT"),
            ConfigurationHelper.SLAVE_REDIS.get("PASSWORD"),
            ConfigurationHelper.SLAVE_REDIS.get("MAX_IDLE"),
            ConfigurationHelper.SLAVE_REDIS.get("MAX_ACTIVE"),
            ConfigurationHelper.SLAVE_REDIS.get("DB"),
            ConfigurationHelper.SLAVE_REDIS.get("CONN_TIMEOUT"),
            ConfigurationHelper.SLAVE_REDIS.get("READ_TIMEOUT"),
            ConfigurationHelper.SLAVE_REDIS.get("MAX_REDIRECTIONS"));

    private String ip;
    private String port;
    private String password;
    private String maxIdle;
    private String maxActive;
    private String db;
    private String connTimeout;
    private String readTimeout;
    private String maxRedirections;

    RedisInfo(String ip, String port, String password, String maxIdle, String maxActive, String db,
              String connTimeout, String readTimeout, String maxRedirections) {
        this.ip = ip;
        this.port = port;
        this.password = password;
        this.maxIdle = maxIdle;
        this.maxActive = maxActive;
        this.db = db;
        this.connTimeout = connTimeout;
        this.readTimeout = readTimeout;
        this.maxRedirections = maxRedirections;
    }

    public String getIp() {return ip;}
    public int getPort() {return StringUtil.isNullOrEmpty(port) ? 6379 : Integer.valueOf(port);}
    public String getPassword() {return password;}
    public int getMaxIdle() {return StringUtil.isNullOrEmpty(maxIdle) ? 3 : Integer.valueOf(maxIdle);}
    public int getMaxActive() {return StringUtil.isNullOrEmpty(maxActive) ? 33 : Integer.valueOf(maxActive);}
    public int getDb() {return StringUtil.isNullOrEmpty(db)? 0 : Integer.valueOf(db);}
    public int getConnTimeout(){return StringUtil.isNullOrEmpty(connTimeout)? 120000 : Integer.valueOf(connTimeout);}
    public int getReadTimeout(){return StringUtil.isNullOrEmpty(readTimeout)? 60000 : Integer.valueOf(readTimeout);}
    public int getMaxRedirections(){return StringUtil.isNullOrEmpty(maxRedirections) ? 5 : Integer.valueOf(maxRedirections);}

    public URI getURI() throws URISyntaxException {

        // redis://[:password@]host[:port][/db-number][?option=value]
        // redis://authstring@192.168.1.1:6379/7

        String url =  String.format("redis://%s%s:%s/%d",
                StringUtil.isNullOrEmpty(password) ? "" : password+"@", ip, getPort(), getDb());

        return new URI(url);
    }

    public GenericObjectPoolConfig getPoolConfig(){
        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxTotal(getMaxActive());
        conf.setMaxIdle(getMaxIdle());
        conf.setTestOnBorrow(true);
        return conf;
    }

    public Set<HostAndPort> getHostAndPort(){
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort(getIp(), getPort()));
        return jedisClusterNodes;
    }

}
