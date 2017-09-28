package com.upsmart.ausync.process.master;

import com.hang.netty.httpwrapper.HttpRequestWrapper;
import com.hang.netty.httpwrapper.HttpResponseWrapper;
import com.hang.netty.processor.HttpProcessor;

/**
 * Created by yuhang on 17-9-28.
 *
 * redis接口查询
 */
public class AuQueryProcessor implements HttpProcessor {
    @Override
    public boolean process(HttpRequestWrapper httpRequestWrapper, HttpResponseWrapper httpResponseWrapper) {
        return false;
    }
}
