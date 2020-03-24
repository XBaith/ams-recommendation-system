package com.ams.recommend;

import com.ams.recommend.client.HBaseClient;

public class HBaseClientTest {

    public static void main(String[] args) {
        System.out.println("log表是否存在：" + HBaseClient.existTable("log"));
    }

}
