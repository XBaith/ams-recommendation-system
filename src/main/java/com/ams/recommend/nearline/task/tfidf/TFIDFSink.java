package com.ams.recommend.nearline.task.tfidf;

import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.common.pojo.SpiderArticle;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.PriorityQueue;

public class TFIDFSink implements SinkFunction<SpiderArticle> {
    @Override
    public void invoke(SpiderArticle article, Context context) throws Exception {
        PriorityQueue<Tuple2<String, Double>> topKeyword = article.getTfidf();
        StringBuilder stringBuilder = new StringBuilder();
        while(!topKeyword.isEmpty()){
            Tuple2<String, Double> tiKV = topKeyword.poll();
            if(topKeyword.size() > 1)
                stringBuilder.append(tiKV.f0 + " ");
            else stringBuilder.append(tiKV.f1);
        }
        MySQLClient.putKeywordById(article.getArticleId(), stringBuilder.toString());
    }
}
