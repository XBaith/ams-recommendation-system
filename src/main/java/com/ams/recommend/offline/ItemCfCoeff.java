package com.ams.recommend.offline;

import com.ams.recommend.client.HBaseClient;
import com.ams.recommend.util.Constants;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 基于协同过滤的文章相关度计算
 *           abs( i ∩ j)
 *      w = ——————————————
 *           sqrt(i || j)
 * @author jackybai
 */
public class ItemCfCoeff {

    /**
     * 计算一个文章和其他相关文章的评分,并将计算结果放入Hbase
     *
     * @param id     文章id
     * @param others 其他文章的id
     */
    public void getSingelItemCfCoeff(String id, List<String> others) throws Exception {
        for (String other : others) {
        	if(id.equals(other)) continue;
            Double score = twoItemCfCoeff(id, other);
            HBaseClient.put(Constants.ARTICLE_CF_TABLE,id, "p",other,score.toString());
        }
    }

    /**
     * 计算两个文章之间的评分
     * @param id
     * @param other
     * @return
     * @throws IOException
     */
    private double twoItemCfCoeff(String id, String other) throws IOException {
        Map<String, String> p1 = HBaseClient.getRow(Constants.ARTICLE_HIS_TABLE, id);
        Map<String, String> p2 = HBaseClient.getRow(Constants.ARTICLE_HIS_TABLE, other);

        int n = p1.size();
        int m = p2.size();
        int sum = 0;
        Double total = Math.sqrt(n * m);
        for (Map.Entry entry : p1.entrySet()) {
            String key = (String) entry.getKey();
            for (Map.Entry p : p2.entrySet()) {
                if (key.equals(p.getKey())) {
                    sum++;
                }
            }
        }
        if (total == 0){
            return 0.0;
        }
        return sum / total;
    }
}
