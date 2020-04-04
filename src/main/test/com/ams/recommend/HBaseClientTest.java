package com.ams.recommend;

import com.ams.recommend.client.HBaseClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.util.Map;

public class HBaseClientTest {

    public static void main(String[] args) {
        System.out.println("log表是否存在：" + HBaseClient.existTable("log"));
        HBaseClient.createTableIfNotExist("log", "l");
        HBaseClient.createTableIfNotExist("u_interest", "i");
    }

    @Test
    public void getTest() {

        Map<String, String> kvs = HBaseClient.getRow("log", "9223372035269505076");

        for(Map.Entry<String, String> kv : kvs.entrySet()) {
            System.out.println("column : " + kv.getKey() + ", value : " + kv.getValue());
        }

    }

}
