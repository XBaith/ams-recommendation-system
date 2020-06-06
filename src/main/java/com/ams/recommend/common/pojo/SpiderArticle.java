package com.ams.recommend.common.pojo;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class SpiderArticle {

    private String articleId;
    private long timestamp;
    private String content;
    private Map<String, Double> tfMap;
    private PriorityQueue<Tuple2<String, Double>> tfidf;

    public SpiderArticle() {
        tfMap = new HashMap<>();
        tfidf = new PriorityQueue();
    }

    public SpiderArticle(String articleId, long timestamp, String content) {
        this.articleId = articleId;
        this.timestamp = timestamp;
        this.content = content;
        tfMap = new HashMap<>();
        tfidf = new PriorityQueue();
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }
    public Map<String, Double> getTfMap() {
        return tfMap;
    }

    public void setTfMap(Map<String, Double> tfMap) {
        this.tfMap = tfMap;
    }

    public PriorityQueue<Tuple2<String, Double>> getTfidf() {
        return tfidf;
    }

    public void setTfidf(PriorityQueue<Tuple2<String, Double>> tfidf) {
        this.tfidf = tfidf;
    }
}
