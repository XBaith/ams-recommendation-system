package com.ams.recommend.util;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordTokenizerUtil {

    public static Map<String, Double> getFrequency(String content) {
        Map<String, Double> wc = new HashMap<>();
        List<Term> terms = NotionalTokenizer.segment(content);
        int wordSize = terms.size();
        System.err.println("总共：" + wordSize + "词");
        for(Term term : terms) {
            if(wc.keySet().contains(term.word)) {
                wc.put(term.word, wc.get(term.word) + 1.0);
            }else wc.put(term.word, 1.0);
        }

        Map<String, Double> tf = new HashMap<>();

        for(Map.Entry<String, Double> w : wc.entrySet()) {
//            System.out.println(w.getKey() + " 出现次数 : " + w.getValue());
            tf.put(w.getKey(), (w.getValue() / wordSize));
        }
        return tf;
    }
}
