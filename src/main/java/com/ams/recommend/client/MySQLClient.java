package com.ams.recommend.client;

import com.alibaba.druid.pool.DruidDataSource;
import com.ams.recommend.util.Property;
import org.apache.flink.api.java.DataSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLClient {

    private final static DruidDataSource dataSource;

    static {
        dataSource = new DruidDataSource();
        dataSource.setUrl(Property.getStrValue("mysql.url"));
        dataSource.setUsername(Property.getStrValue("mysql.name"));
        dataSource.setPassword(Property.getStrValue("mysql.password"));
    }

    /**
     * 根据文章id查询文章内容
     * @param articleId 文章id
     * @return 文章内容
     */
    public static String getContentById(String articleId) {
        String content = "";
        try(Connection conn = dataSource.getConnection()) {
            PreparedStatement pst = conn.prepareStatement("SELECT content FROM article WHERE id = ?");
            pst.setString(1, articleId);
            ResultSet rs = pst.executeQuery();
            content = rs.getString("content");

            pst.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return content;
    }

    /**
     * 根据文章id查询得到所有文章属性
     * @param articleId 文章id
     * @return 文章属性
     */
    public static ResultSet getArticleById(String articleId) {
        ResultSet article = null;
        try(Connection conn = dataSource.getConnection()) {
            PreparedStatement pst = conn.prepareStatement("SELECT * FROM article WHERE id = ?");
            pst.setString(1, articleId);
            article = pst.executeQuery();

            pst.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return article;
    }

    /**
     * 根据用户id查询得到所有用户属性
     * @param userId 用户id
     * @return 用户属性
     */
    public static ResultSet getUserById(String userId) {
        ResultSet user = null;
        try(Connection conn = dataSource.getConnection()) {
            PreparedStatement pst = conn.prepareStatement("SELECT * FROM user WHERE id = ?");
            pst.setString(1, userId);
            user = pst.executeQuery();

            pst.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return user;
    }

    public static void putKeywordById(String id, String keyword) {
        try(Connection conn = dataSource.getConnection()) {
            PreparedStatement pst = conn.prepareStatement("INSERT INTO article(keyword) VALUES(?) WHERE id = ?");
            pst.setString(1, keyword);
            pst.setString(2, id);
            pst.executeUpdate();

            pst.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
