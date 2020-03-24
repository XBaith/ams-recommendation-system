package com.ams.recommend.task;

import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.pojo.SpiderArticle;
import com.ams.recommend.pojo.WindowedArticle;
import com.ams.recommend.tfidf.TFIDFMapFunction;
import com.ams.recommend.tfidf.TFIDFSink;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class SpiderTask {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "spider",
                new SimpleStringSchema(),
                Property.getKafkaProperties("tf-idf")
        );

//        DataStream<List<String>> tfidfWords = env.addSource(consumer)
//                .map(new SpiderMapFunction())
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SpiderArticle>(Time.seconds(1)) {
//                    @Override
//                    public long extractTimestamp(SpiderArticle element) {
//                        return element.getTimestamp() * 1000;   //转化为毫秒
//                    }
//                }).timeWindowAll(Time.minutes(1), Time.seconds(5))
//                .apply(new SpiderWindowFunction())
//                .keyBy(WindowedArticle::getWindowEnd)
//                .process(new TFIDFProcessFunction(5));

//        tfidfWords.addSink(new TFIDFSink());

        env.addSource(consumer)
                .map(new SpiderMapFunction())
                .map(new TFIDFMapFunction(15))
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

    private static class SpiderWindowFunction implements AllWindowFunction<SpiderArticle, WindowedArticle, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<SpiderArticle> spiderArticle, Collector<WindowedArticle> out) throws Exception {
            SpiderArticle article = spiderArticle.iterator().next();
            out.collect(new WindowedArticle(article.getArticleId(), article.getTimestamp(), window.getEnd()));
        }
    }

}
