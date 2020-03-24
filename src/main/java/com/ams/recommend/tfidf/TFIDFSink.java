package com.ams.recommend.tfidf;

import com.ams.recommend.client.MySQLClient;
import com.ams.recommend.pojo.SpiderArticle;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.PriorityQueue;

public class TFIDFSink implements SinkFunction<SpiderArticle> {
    @Override
    public void invoke(SpiderArticle article, Context context) throws Exception {
        PriorityQueue<String> topKeyword = article.getTfidf();
        StringBuilder stringBuilder = new StringBuilder();
        while(!topKeyword.isEmpty()){
            if(topKeyword.size() > 1)
                stringBuilder.append(topKeyword + ",");
            else stringBuilder.append(topKeyword);
        }
        MySQLClient.putKeywordById(article.getArticleId(), stringBuilder.toString());
    }
}
