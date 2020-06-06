package com.ams.recommend.nearline.task.tfidf;

import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.common.pojo.SpiderArticle;
import com.ams.recommend.nearline.task.HotArticleTask;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpiderTask {

    private final static Logger logger = LoggerFactory.getLogger(HotArticleTask.class);
    private static final Integer KEYWORD_SIZE = 10; //爬去文章筛选的关键字个数

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
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SpiderArticle>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(SpiderArticle element) {
                        logger.info("spider article watermark : " + element.getTimestamp());
                        return element.getTimestamp();
                    }
                })
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
