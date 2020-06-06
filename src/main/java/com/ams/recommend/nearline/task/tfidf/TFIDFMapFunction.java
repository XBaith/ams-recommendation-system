package com.ams.recommend.nearline.task.tfidf;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.common.pojo.SpiderArticle;
import com.ams.recommend.util.Constants;
import com.ams.recommend.util.Property;
import com.ams.recommend.util.WordTokenizerUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;
import java.util.PriorityQueue;

public class TFIDFMapFunction implements MapFunction<SpiderArticle, SpiderArticle> {

    private final String tableName = Property.getStrValue("table.word.name");
    private int keywordSize;
    private long totalArticleSize = 1L;

    public TFIDFMapFunction(int keywordSize) {
        if(keywordSize < 1) throw new IllegalArgumentException("keywords num should not less than 1.");
        this.keywordSize = keywordSize;

        //取出文章总数
        String sizeStr = HBaseClient.get(tableName, "articleSize", "c", "count");
        if(sizeStr != null) totalArticleSize = Long.valueOf(sizeStr);
    }

    @Override
    public SpiderArticle map(SpiderArticle article) throws Exception {
        //统计文章各个词的TF
        Map<String, Double> tf = WordTokenizerUtil.tf(article.getContent());
        article.setTfMap(tf);

        PriorityQueue<Tuple2<String, Double>> tfidfQueue = new PriorityQueue<>(keywordSize);

        //计算TF-IDF
        for(String word : tf.keySet()) {
            //查询包含词word的文章总数
            int size = HBaseClient.getColumnSize(tableName, word, "a");
            if(size == 0) size = 1;
            Double TF = tf.get(word);
            Double IDF = Math.log10(totalArticleSize / size);
            Double tfidf = TF * IDF;
            tfidfQueue.add(new Tuple2<>(word, tfidf));
            //更新单词(rowKey)对应的文章列
            HBaseClient.addOrUpdateColumn(tableName, word, "a", article.getArticleId());
            //更新文章中各个单词的tf,tfidf
            HBaseClient.put(Constants.ARTICLE_TFIDF_TABLE, article.getArticleId(), "tf", word, String.valueOf(tf));
            HBaseClient.put(Constants.ARTICLE_TFIDF_TABLE, article.getArticleId(), "ti", word, String.valueOf(tfidf));
        }
        article.setTfidf(tfidfQueue);
        //更新总文章数
        HBaseClient.addOrUpdateColumn(tableName, "articleSize", "c", "count");

        return article;
    }

}
