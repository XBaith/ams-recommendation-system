package com.ams.recommend.pojo;

import java.util.HashMap;
import java.util.Map;

public class WindowedArticle {

    private String articleId;
    private long timestamp;
    private long windowEnd;
    private Map<String, Double> tfMap;

    public WindowedArticle() {
        tfMap = new HashMap<>();
    }

    public WindowedArticle(String articleId, long timestamp, long windowEnd) {
        this.articleId = articleId;
        this.timestamp = timestamp;
        this.windowEnd = windowEnd;
        tfMap = new HashMap<>();
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

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Map<String, Double> getTfMap() {
        return tfMap;
    }

    public void setTfMap(Map<String, Double> tfMap) {
        this.tfMap = tfMap;
    }
}
