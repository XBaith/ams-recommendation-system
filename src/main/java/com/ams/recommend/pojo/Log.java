package com.ams.recommend.pojo;

/**
 * 日志实体
 */
public class Log {

    private String userId;
    private String articleId;
    private Long time;
    private String action;

    public String getUserId() { return userId; }

    public void setUserId(String userId) { this.userId = userId; }

    public String getArticleId() { return articleId; }

    public void setArticleId(String articleId) { this.articleId = articleId; }

    public void setArticleId(int productId) {
        this.articleId = articleId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

}
