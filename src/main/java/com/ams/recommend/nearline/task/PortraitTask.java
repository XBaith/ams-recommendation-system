package com.ams.recommend.nearline.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.common.pojo.Log;
import com.ams.recommend.common.pojo.User;
import com.ams.recommend.util.Constants;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import com.ams.recommend.util.WordTokenizerUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.ResultSet;

/**
 * 画像Task
 */
public class PortraitTask {

    private final static Long TTL = 180L;  //判定用户喜欢文章的阅读时间 默认3min

    private static final OutputTag<Log> outputTag = new OutputTag<Log>("side-output"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> logSource = env.addSource(new FlinkKafkaConsumer<>(
                "log",
                new SimpleStringSchema(),
                Property.getKafkaProperties("portrait")
        ));

        SingleOutputStreamOperator<Log> logs = logSource
                .process(new LogProcessFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Log>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Log element) {
                        return element.getTime();
                    }
                });

        logs.keyBy("articleId")
                .addSink(new ArticlePortraitSink());

        logs.getSideOutput(outputTag)
                .keyBy("userID")
                .addSink(new UserPortraitSink());

        env.execute("Portrait Task");
    }

    private static class LogProcessFunction extends ProcessFunction<String, Log> {
        @Override
        public void processElement(String log, Context ctx, Collector<Log> out) throws Exception {
            Log logEntry = LogUtil.toLogEntry(log);
            out.collect(logEntry);
            //侧输出到另外一条Stream
            ctx.output(outputTag, logEntry);
        }
    }

    /**
     * 统计喜欢看这篇文章的用户信息，比如：性别，年龄，职业，学历等
     */
    private static class ArticlePortraitSink implements SinkFunction<Log> {
        @Override
        public void invoke(Log log, Context context) throws Exception {
            User user = MySQLClient.getUserById(log.getUserId());
            String articleId = log.getArticleId();
            String userId = log.getUserId();
            //性别
            HBaseClient.put(Constants.ARTICLE_PORTRAIT_TABLE,
                    articleId,
                    "sex",
                    userId,
                    String.valueOf(user.getSex())
            );
            //年龄段
            HBaseClient.put(Constants.ARTICLE_PORTRAIT_TABLE,
                    articleId,
                    "age",
                    userId,
                    Constants.rangeAge(user.getAge())
            );
            //职业
            HBaseClient.put(Constants.ARTICLE_PORTRAIT_TABLE,
                    articleId,
                    "job",
                    userId,
                    user.getJob()
            );
            //学历
            HBaseClient.put(Constants.ARTICLE_PORTRAIT_TABLE,
                    articleId,
                    "edu",
                    userId,
                    user.getEducation()
            );
        }
    }

    /**
     * 统计用户画像信息
     * 作者（文章来源），频道，标题，关键字
     */
    private static class UserPortraitSink implements SinkFunction<Log> {
        @Override
        public void invoke(Log log, Context context) throws Exception {
            ResultSet rs = MySQLClient.getUserPortraitById(log.getArticleId());

            if(rs != null) {
                rs.next();
                String author = rs.getString("author");
                int channelId = rs.getInt("channel_id");
                String title = rs.getString("title");
                String keyword = rs.getString("keyword");

                String userId = log.getUserId();
                String articleId = log.getArticleId();
                //作者
                HBaseClient.put(Constants.USER_PORTRAIT_TABLE,
                        userId,
                        "aut",
                        articleId,
                        author
                );
                //频道
                HBaseClient.put(Constants.USER_PORTRAIT_TABLE,
                        userId,
                        "cha",
                        articleId,
                        String.valueOf(channelId)
                );
                //标题
                HBaseClient.put(Constants.USER_PORTRAIT_TABLE,
                        userId,
                        "tit",
                        articleId,
                        WordTokenizerUtil.segment(title)
                );
                //关键字
                HBaseClient.put(Constants.USER_PORTRAIT_TABLE,
                        userId,
                        "kw",
                        articleId,
                        keyword
                );
            }
        }
    }

}
