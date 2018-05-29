package com.cognitree.flume.sink.elasticsearch.filter;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @description: 把body中id拿出来加在head中
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-09
 **/
public class BodyGetIdInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(BodyGetIdInterceptor.class);

    private static final String ID="id";

    private static String PRIMARY_KEY;
    private static String FILTER_KEY;
    private static String FILTER_VALUE;

    private static final String PRIMARYKEY="primaryKey";
    private static final String FILTERKEY="filterKey";
    private static final String FILTERVALUE="filterValue";

    @Override
    public void initialize() {
        logger.info("BodyGetIdInterceptor init...");
    }

    private JsonParser jsonParser = new JsonParser();

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), Charsets.UTF_8);
        logger.debug("body {}",body);
        try {
            JsonObject json = jsonParser.parse(body).getAsJsonObject();
            // 为空不做过滤
            if (FILTER_KEY == null || FILTER_VALUE == null){
                return event;
            }
            String result=null;
            if (json.has(FILTER_KEY) && FILTER_VALUE.equals(json.get(FILTER_KEY).getAsString())){
                String[] split = PRIMARY_KEY.split("\\.");
                for (int i=0;i<split.length;i++){
                    if (i==split.length-1){
                        result=json.get(split[i]).getAsString();
                    }else {
                        json = json.get(split[i]).getAsJsonObject();
                    }
                }
            }
            if (result != null){
                headers.put(ID, result);
            }

        } catch (JsonSyntaxException e) {
            logger.error("parse json fail :{}",body);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }
    private BodyGetIdInterceptor(){

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new BodyGetIdInterceptor();
        }


        @Override
        public void configure(Context context) {
            PRIMARY_KEY=context.getString(PRIMARYKEY,ID);
            FILTER_KEY =context.getString(FILTERKEY,ID);
            FILTER_VALUE =context.getString(FILTERVALUE,ID);
            logger.info("primary_key:[{}]",PRIMARY_KEY);

        }
    }
}
