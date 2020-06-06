package com.ams.recommend.util;

import com.ams.recommend.common.pojo.Log;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class LogUtilTest {

    @Test
    public void testToLogEntry() {
        Log log = new Log();
        log.setUserId("1");
        log.setArticleId("1");
        long timestamp = new Date().getTime();
        log.setTime(timestamp);
        log.setAction("1"); //游览操作

        Assert.assertEquals(log.toString(), LogUtil.toLogEntry("1,1," + timestamp + ",1").toString());
    }

    @Test
    public void rowKey() {
        long timestamp = new Date().getTime();
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE - timestamp), LogUtil.getLogRowKey(timestamp));
    }

}
