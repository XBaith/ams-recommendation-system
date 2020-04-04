package com.ams.recommend.task;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.pojo.Log;
import com.ams.recommend.util.LogUtil;
import com.ams.recommend.util.Property;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LogTask {

    private static final Logger logger = LoggerFactory.getLogger(LogTask.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(5000L);  //设置每5秒设置自动生成checkpoint

        Properties kafkaProp = Property.getKafkaProperties("log");  //设置消费组id
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log",
                new SimpleStringSchema(),
                kafkaProp
        );

        DataStream<Log> logs = env
                .addSource(consumer)
                .flatMap(new LogFlatMapFunction());

        env.execute("Collect log task");
    }

    private static class LogFlatMapFunction implements FlatMapFunction<String, Log> {

        @Override
        public void flatMap(String value, Collector<Log> out) throws Exception {
            //直接将Kafka传来的log放入HBase中
            Log log = LogUtil.toLogEntry(value);    //将log转化为Log实体

            if(log != null) {
                final String rowKey = LogUtil.getLogRowKey(log.getTime());
                String tableName = Property.getStrValue("table.log.name");
                //如果不存在表就创建一个
                HBaseClient.createTableIfNotExist(tableName, "l");
                //插入用户id
                HBaseClient.put(tableName, rowKey, "l"
                        , "uid", log.getUserId());
                //插入文章id
                HBaseClient.put(tableName, rowKey, "l"
                        , "aid", log.getArticleId());
                //插入用户发生动作时间
                HBaseClient.put(tableName, rowKey, "l"
                        , "ts", String.valueOf(log.getTime()));
                //插入用户发生的动作
                HBaseClient.put(tableName, rowKey, "l"
                        , "act", log.getAction());

                logger.info(log.toString());
                out.collect(log);
            }
        }
    }

}


