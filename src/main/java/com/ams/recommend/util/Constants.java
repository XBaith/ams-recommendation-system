package com.ams.recommend.util;

public class Constants {

    public static final String SEX_MAN = "1";
    public static final String SEX_WOMAN = "0";
    public static final String AGE_10 = "10s";
    public static final String AGE_20 = "20s";
    public static final String AGE_30 = "30s";
    public static final String AGE_40 = "40s";
    public static final String AGE_50 = "50s";
    public static final String AGE_60 = "60s";

    public static String rangeAge(int age) {
        if(age < 20) return AGE_10;
        else if(age < 30) return AGE_20;
        else if(age < 30) return AGE_30;
        else if(age < 40) return AGE_40;
        else if(age < 50) return AGE_50;
        else return AGE_60;
    }

    /*文章画像表*/
    public final static String ARTICLE_PORTRAIT_TABLE = Property.getStrValue("table.portrait.article.name");
    /*用户画像表*/
    public final static String USER_PORTRAIT_TABLE = Property.getStrValue("table.portrait.user.name");
    /*文章单词表*/
    public final static String WORD_TABLE = Property.getStrValue("table.word.name");
    /*文章历史信息表*/
    public final static String ARTICLE_HIS_TABLE =  Property.getStrValue("table.article.history.name");
    /*文章历史信息表*/
    public final static String USER_HIS_TABLE =  Property.getStrValue("table.user.history.name");
    /*文章相关度表*/
    public final static String ARTICLE_CF_TABLE =  Property.getStrValue("table.article.cf.name");
    /*文章标签相关度表*/
    public final static String ARTICLE_TAG_TABLE =  Property.getStrValue("table.article.tag.name");
    /*TF-IDF*/
    public final static String ARTICLE_TFIDF_TABLE =  Property.getStrValue("table.article.tfidf.name");
}
