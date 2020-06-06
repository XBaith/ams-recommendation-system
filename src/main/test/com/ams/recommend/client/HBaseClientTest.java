package com.ams.recommend.client;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HBaseClientTest {

    @Test
    public void testTableApi() {
        Assert.assertEquals(true, HBaseClient.existTable("log"));
        HBaseClient.createTableIfNotExist("log", "l");
        HBaseClient.createTableIfNotExist("u_interest", "i");
    }

    @Test
    public void testGetRow() {
        Map<String, String> kvs = HBaseClient.getRow("log", "9223372035269505076");

        for(Map.Entry<String, String> kv : kvs.entrySet()) {
            System.out.println("column : " + kv.getKey() + ", value : " + kv.getValue());
        }
    }

    @Test
    public void testPut() {
        HBaseClient.put("log",
                "9223372035269505076",
                "l",
                "uid",
                "50");
    }

    @Test
    public void testGet() {
        String res = HBaseClient.get("log",
                "9223372035269505076",
                "l",
                "uid"
        );
        Assert.assertEquals("50", res);
    }

}