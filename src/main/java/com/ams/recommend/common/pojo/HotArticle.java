package com.ams.recommend.common.pojo;

public class HotArticle {

    private String articleId;   //文章id
    private long pvCount;   //文章游览量
    private long windowEnd; //窗口结束时间戳
    private int rank;   //热度榜名次

    public HotArticle() {
    }

    public HotArticle(String articleId, long pvCount, long windowEnd, int rank) {
        this.articleId = articleId;
        this.pvCount = pvCount;
        this.windowEnd = windowEnd;
        this.rank = rank;
    }

    public HotArticle(String articleId, long pvCount, long windowEnd) {
        this.articleId = articleId;
        this.pvCount = pvCount;
        this.windowEnd = windowEnd;
        this.rank = 0;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public long getPvCount() {
        return pvCount;
    }

    public void setPvCount(long pvCount) {
        this.pvCount = pvCount;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return "HotArticle : " +
                "articleId='" + articleId + '\'' +
                ", pvCount=" + pvCount +
                ", windowEnd=" + windowEnd +
                ", rank=" + rank;
    }
}
