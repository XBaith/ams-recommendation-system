package com.ams.recommend.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.pojo.Log;
import com.ams.recommend.pojo.User;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.ResultSet;

/**
 * 画像Task
 */
public class PortraitTask {

    private final static String ARTICLE_PORTRAIT_TABLENAME = Property.getStrValue("table.portrait.article.name");
    private final static String USER_PORTRAIT_TABLENAME = Property.getStrValue("table.portrait.user.name");

    private static final OutputTag<Log> outputTag = new OutputTag<Log>("side-output"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> logSource = env.addSource(new FlinkKafkaConsumer<>(
                "log",
                new SimpleStringSchema(),
                Property.getKafkaProperties("portrait")
        ));

        SingleOutputStreamOperator<Log> logs = logSource.process(new LogProcessFunction());

        logs.addSink(new ArticlePortraitSink());

        logs.getSideOutput(outputTag)
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
            HBaseClient.put(ARTICLE_PORTRAIT_TABLENAME,
                    articleId,
                    "s",
                    userId,
                    String.valueOf(user.getSex())
            );
            //年龄
            HBaseClient.put(ARTICLE_PORTRAIT_TABLENAME,
                    articleId,
                    "a",
                    userId,
                    String.valueOf(user.getAge())
            );
            //职业
            HBaseClient.put(ARTICLE_PORTRAIT_TABLENAME,
                    articleId,
                    "j",
                    userId,
                    user.getJob()
            );
            //学历
            HBaseClient.put(ARTICLE_PORTRAIT_TABLENAME,
                    articleId,
                    "e",
                    userId,
                    user.getEducation()
            );
        }
    }

    /**
     * 统计用户画像信息
     * 作者，频道，主题，关键字
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
                HBaseClient.put(USER_PORTRAIT_TABLENAME,
                        userId,
                        "a",
                        articleId,
                        author
                );
                //频道
                HBaseClient.put(USER_PORTRAIT_TABLENAME,
                        userId,
                        "c",
                        articleId,
                        String.valueOf(channelId)
                );
                //标题
                HBaseClient.put(USER_PORTRAIT_TABLENAME,
                        userId,
                        "t",
                        articleId,
                        title
                );
                //关键字
                HBaseClient.put(USER_PORTRAIT_TABLENAME,
                        userId,
                        "k",
                        articleId,
                        keyword
                );
            }
        }
    }
}
