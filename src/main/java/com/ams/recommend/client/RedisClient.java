package com.ams.recommend.client;


import com.ams.recommend.util.Property;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class RedisClient {
    private static Jedis jedis;

    static {
    	jedis = new Jedis(Property.getStrValue("redis.host"), Property.getIntValue("redis.port"));
		jedis.select(Property.getIntValue("redis.db"));
	}

    /**
     * 获取redis中对应的值
     * @param key　建
     * @return  值
     */
    public String getData(String key){
        return jedis.get(key);
    }

    /**
     * 获取热榜文章
     * @param topRange 热门文章数
     * @return  热门文章id
     */
    public List<String> getTopList(int topRange){
        List<String> res = new ArrayList<>();
        for (int i = 0; i < topRange; i++) {
            res.add(getData(String.valueOf(i)));
        }
        return res;
    }

}
