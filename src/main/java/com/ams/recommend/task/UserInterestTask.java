package com.ams.recommend.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.pojo.Log;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class UserInterestTask {

    private final static Long TTL = 180L;  //判定用户喜欢文章的阅读时间 默认3min:180_1000L

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "log",
                new SimpleStringSchema(),
                Property.getKafkaProperties("user-interest")
        );

        env.addSource(consumer)
                .map(new LogMapFunction())
                .keyBy(user -> user.getUserId())
                .addSink(new UserInterestSinkFunction());

        env.execute("User Interest Task");
    }

    private static class LogMapFunction implements MapFunction<String, Log> {
        @Override
        public Log map(String value) throws Exception {
            Log log = LogUtil.toLogEntry(value);

            if(log != null) return log;
            else return null;
        }
    }

    private static class UserInterestSinkFunction extends RichSinkFunction<Log> {

        private ValueState<Long> lastTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //设置状态过期时间为5个小时（粗略认为如果用户5个小时还没有关闭该页面，可能是挂机状态，不认为是有效游览）
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(5))
                    .build();

            ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("Open Page time", Long.class);
            desc.enableTimeToLive(ttlConfig);
            lastTimeState = getRuntimeContext().getState(desc);
        }

        @Override
        public void invoke(Log log, Context context) throws Exception {
            //动作: 1.打开游览;2.点赞;3.收藏;4.关闭
            String op = log.getAction();
            Long curTime = log.getTime();
            Long lastTime = lastTimeState.value();
            if(lastTime == null || "1".equals(op)) {
                lastTimeState.update(curTime);
            }else if("4".equals(op) && lastTimeState.value() != null) {
                lastTime = lastTimeState.value();
                if(curTime - lastTime > TTL) {  //游览时间大于TTL，表示用户喜欢该文章
                    HBaseClient.addOrUpdateColumn("user_portrait", log.getUserId(), "i", log.getArticleId());
                }
                lastTimeState.clear();
            }
        }

    }

}
