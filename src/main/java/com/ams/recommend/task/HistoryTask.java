package com.ams.recommend.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.pojo.Log;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class HistoryTask {

    //文章历史信息表
    private final static String ARTICLE_HIS_TABLE_NAME =  Property.getStrValue("table.article.history.name");
    //用户历史信息表
    private final static String USER_HIS_TABLE_NAME =  Property.getStrValue("table.user.history.name");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "log",
                new SimpleStringSchema(),
                Property.getKafkaProperties("history")
        );

        env.addSource(consumer)
                .flatMap((FlatMapFunction<String, Object>) (value, out) -> {
                    Log log = LogUtil.toLogEntry(value);
                    if(null != log) {
                        //文章相对应的用户操作更新1次记录
                        HBaseClient.addOrUpdateColumn(
                                ARTICLE_HIS_TABLE_NAME,
                                log.getArticleId(),
                                "p",
                                log.getUserId());
                        //用户对游览的文章的操作次数加1
                        HBaseClient.addOrUpdateColumn(
                                USER_HIS_TABLE_NAME,
                                log.getUserId(),
                                "p",
                                log.getArticleId());
                    }
                });

        env.execute("History Task");
    }

}
