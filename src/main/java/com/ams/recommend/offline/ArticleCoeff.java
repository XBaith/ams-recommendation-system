package com.ams.recommend.offline;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.common.pojo.ArticlePortrait;
import com.ams.recommend.util.Constants;

import java.io.IOException;
import java.util.List;

/**
 * 基于文章标签的文章相关度计算
 * 1.基于文章标签 计算文章的余弦相似度
 * 2.基于文章内容（关键字） 计算文章的相似度
 * @author jackybai
 */
public class ArticleCoeff {
	/**
	 * 计算一个文章和其他相关文章的评分,并将计算结果放入Hbase
	 * @param id 文章id
	 * @param others 其他文章的id
	 */
	public void getArticleCoeff(String id, List<String> others) throws Exception {
		ArticlePortrait article = sigleArticle(id);
		for (String articleId : others) {
			if (id.equals(articleId)) continue;
			ArticlePortrait entity = sigleArticle(articleId);
			Double score = getScore(article, entity);
			HBaseClient.put(Constants.ARTICLE_TAG_TABLE, id, "p", articleId, score.toString());
		}
	}

	/**
	 * 获取一个文章的所有标签数据
	 * @param articleId 文章id
	 * @return 文章标签entity
	 * @throws IOException
	 */
	private ArticlePortrait sigleArticle(String articleId) {
		ArticlePortrait entity = new ArticlePortrait();
		try {
			String woman = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "sex", Constants.SEX_WOMAN);
			String man = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "sex", Constants.SEX_MAN);
			String age_10 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_10);
			String age_20 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_20);
			String age_30 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_30);
			String age_40 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_40);
			String age_50 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_50);
			String age_60 = HBaseClient.get(Constants.ARTICLE_PORTRAIT_TABLE, articleId, "age", Constants.AGE_60);
			entity.setMan(Integer.valueOf(man));
			entity.setWoman(Integer.valueOf(woman));
			entity.setAge_10(Integer.valueOf(age_10));
			entity.setAge_20(Integer.valueOf(age_20));
			entity.setAge_30(Integer.valueOf(age_30));
			entity.setAge_40(Integer.valueOf(age_40));
			entity.setAge_50(Integer.valueOf(age_50));
			entity.setAge_60(Integer.valueOf(age_60));
		} catch (Exception e) {
			System.err.println("articleId: " + articleId);
			e.printStackTrace();
		}
		return entity;
	}

	/**
	 * 根据标签计算两个文章之间的相关度
	 * @param article 文章
	 * @param target 相关文章
	 * @return 相似分数
	 */
	private double getScore(ArticlePortrait article, ArticlePortrait target) {
		double sqrt = Math.sqrt(article.getTotal() + target.getTotal());
		if (sqrt == 0) {
			return 0.0;
		}
		int total = article.getMan() * target.getMan() + article.getWoman() * target.getWoman()
				+ article.getAge_10() * target.getAge_10() + article.getAge_20() * target.getAge_20()
				+ article.getAge_30() * target.getAge_30() + article.getAge_40() * target.getAge_40()
				+ article.getAge_50() * target.getAge_50() + article.getAge_60() * target.getAge_60();
		return Math.sqrt(total) / sqrt;
	}

	public void calcuSimilar(String id, List<String> others) {

	}
}
