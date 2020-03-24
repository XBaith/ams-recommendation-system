package com.ams.recommend.tfidf;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.pojo.SpiderArticle;
import com.ams.recommend.util.Property;
import com.ams.recommend.util.WordTokenizerUtil;
import org.apache.flink.api.common.functions.MapFunction;
import scala.Tuple2;

import java.util.Map;
import java.util.PriorityQueue;

public class TFIDFMapFunction implements MapFunction<SpiderArticle, SpiderArticle> {

    private final String tableName = Property.getStrValue("table.word.name");
    private int keywordSize;
    private Long totalArticleSize = 1L;

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
        Map<String, Double> tf = WordTokenizerUtil.getFrequency(article.getContent());
        article.setTfMap(tf);

        //查询包含词w的文章总数
        PriorityQueue<Tuple2<String, Double>> tfidfQueue = new PriorityQueue<>(keywordSize);

        //先查看HBase中是否有对应的表
        checkWordTable(tableName);

        //计算TF-IDF
        for(String word : tf.keySet()) {
            int size = HBaseClient.getColumnSize(tableName, word, "a");
            if(size == 0) size = 1;
            Double TF = tf.get(word);
            Double IDF = Math.log10(totalArticleSize / size);
            tfidfQueue.add(new Tuple2<String, Double>(word, TF * IDF));
            //更新单词(rowKey)对应的文章列
            HBaseClient.addOrUpdateColumn(tableName, word, "a", article.getArticleId());
        }
        article.setTfidf(tfidfQueue);
        //更新总文章数
        HBaseClient.addOrUpdateColumn(tableName, "articleSize", "c", "count");

        return article;
    }

    private void checkWordTable(String tableName) {
        if(!HBaseClient.existTable(tableName)) {
            HBaseClient.createOrOverwriteTable(tableName, "a", "c");
            HBaseClient.createRow(tableName, "articleSize", "c", "count", String.valueOf(0L));
        }
    }
}
