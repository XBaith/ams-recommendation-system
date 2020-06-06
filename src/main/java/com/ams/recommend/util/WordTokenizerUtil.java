package com.ams.recommend.util;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordTokenizerUtil {

    /**
     * 计算文章中词汇的TF
     */
    public static Map<String, Double> tf(String content) {
        Map<String, Double> wc = new HashMap<>();
        List<Term> terms = NotionalTokenizer.segment(content);
        int wordSize = terms.size();
        System.out.println("总共：" + wordSize + "词");

        for(Term term : terms) {
            if(wc.keySet().contains(term.word)) {
                wc.put(term.word, wc.get(term.word) + 1.0);
            }else wc.put(term.word, 1.0);
        }

        Map<String, Double> tf = new HashMap<>();

        for(Map.Entry<String, Double> w : wc.entrySet()) {
            tf.put(w.getKey(), (w.getValue() / wordSize));
        }
        return tf;
    }

    /**
     * 分词并过滤停用词
     */
    public static String segment(String text) {
        StringBuilder builder = new StringBuilder();
        for(Term term : NotionalTokenizer.segment(text)) {
            builder.append(term.word + " ");
        }
        return builder.toString();
    }
}
