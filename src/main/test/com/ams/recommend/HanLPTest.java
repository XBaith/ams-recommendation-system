package com.ams.recommend;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

public class HanLPTest {

    public static void main(String[] args) {
        List<Term> terms = HanLP.segment("江西鄱阳湖干枯，中国最大淡水湖变成大草原");
        List<Term> termss = NotionalTokenizer.segment("江西鄱阳湖干枯，中国最大淡水湖变成大草原");
        System.out.println(terms);
        System.out.println();
        System.out.println(termss);
    }

}
