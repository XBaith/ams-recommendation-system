package com.ams.recommend;

import com.ams.recommend.client.RedisClient;
import org.junit.Test;

import java.util.List;

public class RedisClientTest {

    @Test
    public void getTest() {
        RedisClient client = new RedisClient();
        int topRange = 10;
        List<String> data = client.getTopList(topRange);
        for(int i = 0; i < topRange; i++)
            System.out.println(i + " : " + data.get(i));
    }

}
