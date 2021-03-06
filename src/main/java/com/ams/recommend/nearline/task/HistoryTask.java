package com.ams.recommend.nearline.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.common.pojo.Log;
import com.ams.recommend.util.Constants;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class HistoryTask {

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
                                Constants.ARTICLE_HIS_TABLE,
                                log.getArticleId(),
                                "p",
                                log.getUserId());
                        //用户对游览的文章的操作次数加1
                        HBaseClient.addOrUpdateColumn(
                                Constants.USER_HIS_TABLE,
                                log.getUserId(),
                                "p",
                                log.getArticleId());
                    }
                });

        env.execute("History Task");
    }

}
