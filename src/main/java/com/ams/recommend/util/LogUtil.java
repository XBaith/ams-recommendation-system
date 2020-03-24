package com.ams.recommend.util;

import com.ams.recommend.pojo.Log;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil {

    private static Logger logger = LoggerFactory.getLogger(LogUtil.class);

    @NotNull
    public static Log toLogEntry(String log) {
        logger.info(log);

        Log logEntry = new Log();

        String[] logArr = log.split(",");   //日志用","作为分隔符，共包括四个部分

        if(logArr.length != 4) {
            logger.error("Log messages is incorrect");
            return null;
        }

        logEntry.setUserId(logArr[0]);
        logEntry.setArticleId(logArr[1]);
        logEntry.setTime(Long.valueOf(logArr[2]));
        logEntry.setAction(logArr[3]);

        return logEntry;
    }

    public static String getLogRowKey(Long time) {
        return String.valueOf(Long.MAX_VALUE - time);
    }

}
