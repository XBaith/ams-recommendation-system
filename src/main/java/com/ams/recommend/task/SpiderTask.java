package com.ams.recommend.task;

import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.pojo.SpiderArticle;
import com.ams.recommend.tfidf.TFIDFMapFunction;
import com.ams.recommend.tfidf.TFIDFSink;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class SpiderTask {

    private static final Integer KEYWORD_SIZE = 20; //爬去文章筛选的关键字个数

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "spider",
                new SimpleStringSchema(),
                Property.getKafkaProperties("tf-idf")
        );

        env.addSource(consumer)
                .map(new SpiderMapFunction())
                .map(new TFIDFMapFunction(KEYWORD_SIZE))
                .addSink(new TFIDFSink());

        env.execute("Spider for tf-idf task");
    }


    private static class SpiderMapFunction implements MapFunction<String, SpiderArticle> {
        @Override
        public SpiderArticle map(String value) throws Exception {
            if(value == null) throw new IllegalArgumentException("Spiders are EMPTY!");

            SpiderArticle article = new SpiderArticle();
            String[] vs = value.split(",");
            String articleId = vs[0];
            long timestamp = Long.valueOf(vs[1]);

            article.setArticleId(articleId);
            article.setTimestamp(timestamp);

            String content = MySQLClient.getContentById(articleId);
            article.setContent(content);

            return article;
        }
    }

}
