package com.ams.recommend.offline;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.nearline.task.LogTask;
import com.ams.recommend.util.Constants;
import org.apache.flink.api.common.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SchedulerJob {
	private static final Logger logger = LoggerFactory.getLogger(SchedulerJob.class);
	private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

	/**
	 * 每12小时定时调度一次 基于三个推荐策略的 文章评分计算
	 * 策略1：协同过滤
	 * 策略2：基于文章标签计算文章的余弦相似度
	 * 策略3：基于文章内容计算文章相似度
	 */
	public static void main(String[] args) {
		Timer qTimer = new Timer();
		qTimer.scheduleAtFixedRate(new RefreshTask(), 0, Time.hours(12).toMilliseconds());
	}

	private static class RefreshTask extends TimerTask {
		@Override
		public void run() {
			logger.info(new Date() + " 开始执行任务");
			/* 取出被用户游览过的文章id */
			List<String> allArticleId;
			try {
				allArticleId = HBaseClient.getAllKey(Constants.ARTICLE_HIS_TABLE);
			} catch (IOException e) {
				System.err.println("获取历史文章id异常: " + e.getMessage());
				e.printStackTrace();
				return;
			}

			for (String id : allArticleId) {
				executorService.execute(new Task(id, allArticleId));
			}
		}
	}

	private static class Task implements Runnable {
		private String id;
		private List<String> others;

		public Task(String id, List<String> others) {
			this.id = id;
			this.others = others;
		}

		ItemCfCoeff item = new ItemCfCoeff();
		ArticleCoeff article = new ArticleCoeff();

		@Override
		public void run() {
			try {
				item.getSingelItemCfCoeff(id, others);	//策略1:基于协同过滤
				article.getArticleCoeff(id, others);	//策略2：基于文章标签
				article.calcuSimilar(id, others);	//策略3：基于文章内容
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
