
package com.cognitree.flume.sink.elasticsearch;

import com.google.gson.*;
import org.apache.avro.data.Json;
import org.junit.Test;

/**
 * @description:
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-09
 **/
public class TestJson {

    @Test
    public void testJson(){
        String body="{\"name\":\"BeJson\",\"url\":\"http://www.bejson.com\",\"page\":88,\"isNonProfit\":true,\"address\":{\"street\":\"科技园路.\",\"city\":\"江苏苏州\",\"country\":\"中国\"},\"links\":[{\"name\":\"Google\",\"url\":\"http://www.google.com\"},{\"name\":\"Baidu\",\"url\":\"http://www.baidu.com\"},{\"name\":\"SoSo\",\"url\":\"http://www.SoSo.com\"}]}";
        JsonElement parse = new JsonParser().parse(body);
        JsonObject asJsonArray = parse.getAsJsonObject();
        asJsonArray.has("id");
        System.out.println(asJsonArray);

        String PRIMARY_KEY="address.city";
        String[] split = PRIMARY_KEY.split("\\.");
        String result=null;
        JsonObject json = new JsonParser().parse(body).getAsJsonObject();

        for (int i=0;i<split.length;i++){
            if (i==split.length-1){
                result=json.get(split[i]).getAsString();
            }else {
                json = json.get(split[i]).getAsJsonObject();
            }
        }
        System.out.println(result);


    }
}
