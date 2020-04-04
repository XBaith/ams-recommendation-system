package com.ams.recommend.task;

import com.ams.recommend.pojo.HotArticle;
import com.ams.recommend.pojo.Log;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class HotArticleTask {

    private final static Logger logger = LoggerFactory.getLogger(HotArticleTask.class);
    private final static int HOTSIZE = 20;  //热榜的文章数

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "log",
                new SimpleStringSchema(),
                Property.getKafkaProperties("hot")
        );

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost(Property.getStrValue("redis.host"))
				.setPort(Property.getIntValue("redis.port"))
				.setDatabase(Property.getIntValue("redis.db"))
                .build();

        env.addSource(consumer)
            .flatMap(new LogFlatMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Log>() {
                    @Override
                    public long extractAscendingTimestamp(Log log) {
                        logger.info("watermark : " + log.getTime() * 1000);
                        return log.getTime() * 1000;    //转化为毫秒
                    }
                }).keyBy(log -> log.getArticleId())
                .timeWindow(Time.seconds(60), Time.seconds(20))
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy(hot -> hot.getWindowEnd())
                .process(new HotArticleProcessFunction(HOTSIZE))
                .flatMap(new TopFlatMapFunction())
        .addSink(new RedisSink<>(redisConf, new HotArticleSink()));

        env.execute("Hot article task");
    }


    private static class LogFlatMapFunction implements FlatMapFunction<String, Log> {
        @Override
        public void flatMap(String value, Collector<Log> out) throws Exception {
            Log log = LogUtil.toLogEntry(value);
            if("1".equals(log.getAction())) {
                out.collect(log);
            }
        }
    }

    private static class CountAgg implements AggregateFunction<Log, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Log value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 将每个key每个窗口聚合后的结果带上热门文章对象进行输出
     */
    private static class WindowResultFunction implements WindowFunction<Long, HotArticle, String, TimeWindow> {
        @Override
        public void apply(String articleId, TimeWindow window, Iterable<Long> input, Collector<HotArticle> out) throws Exception {
            Long pvCount = input.iterator().next();
            HotArticle article = new HotArticle(articleId, pvCount, window.getEnd());
            out.collect(article);

            logger.info(article.toString());
        }
    }

    private static class HotArticleProcessFunction extends KeyedProcessFunction<Long, HotArticle, List<HotArticle>> {

        private int hotSize;
        private ListState<HotArticle> hotArticleListState;

        public HotArticleProcessFunction(int hotSize) {
            if(hotSize < 1) throw new IllegalArgumentException("Article size should not less than 1!");
            this.hotSize = hotSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<HotArticle> hotArticleListStateDescriptor =
                    new ListStateDescriptor<>("hotArticle-state", HotArticle.class);
            hotArticleListState = getRuntimeContext().getListState(hotArticleListStateDescriptor);
        }

        @Override
        public void processElement(HotArticle hotArticle, Context ctx, Collector<List<HotArticle>> out) throws Exception {
            hotArticleListState.add(hotArticle);
            //注册下一次的事件计时器
            ctx.timerService().registerEventTimeTimer(hotArticle.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<HotArticle>> out) throws Exception {
            PriorityQueue<HotArticle> hotArticles = new PriorityQueue<>(hotSize, new Comparator<HotArticle>() {
                @Override
                public int compare(HotArticle o1, HotArticle o2) {
                    if(o1.getPvCount() > o2.getPvCount()) return -1;
                    else if(o1.getPvCount() < o2.getPvCount()) return 1;
                    else return 0;
                }
            });

            for(HotArticle hotArticle : hotArticleListState.get()) {
                hotArticles.add(hotArticle);
            }
            //清理状态
            hotArticleListState.clear();

            out.collect(new LinkedList<>(hotArticles));
        }
    }

    private static class HotArticleSink implements RedisMapper<HotArticle> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, null);
        }

        @Override
        public String getKeyFromData(HotArticle hotArticle) {
            logger.info("Redis Key : " + hotArticle.getRank());
            return String.valueOf(hotArticle.getRank());
        }

        @Override
        public String getValueFromData(HotArticle hotArticle) {
            logger.info("Redis Value : " + hotArticle.getRank());
            return hotArticle.getArticleId();
        }
    }

    private static class TopFlatMapFunction implements FlatMapFunction<List<HotArticle>, HotArticle>{
        @Override
        public void flatMap(List<HotArticle> topArticles, Collector<HotArticle> out) throws Exception {
            StringBuilder builder = new StringBuilder();

            builder.append("\n========== Hot Articles ==========\n");
            int rank = 1;
            for(HotArticle topArticle : topArticles) {
                topArticle.setRank(rank++);
                builder.append("Article ID: " + topArticle.getArticleId())
                        .append(", Rank: " + topArticle.getRank())
                        .append(", PV: " + topArticle.getPvCount() + "\n");

                out.collect(topArticle);
            }

            logger.info(builder.toString());
        }
    }
}
