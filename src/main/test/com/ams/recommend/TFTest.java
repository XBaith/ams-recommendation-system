package com.ams.recommend;

import com.ams.recommend.util.WordTokenizerUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class TFTest {

    public static void main(String[] args) throws FileNotFoundException {
        StringBuilder sb = new StringBuilder();

        Scanner in = new Scanner(
                new File("/media/baith/123b86d4-6a94-41c8-994f-5786ea4c760c/download/bi.txt")
        );

        while(in.hasNext()) {
            sb.append(in.next());
        }

        Map<String, Double> tfs = WordTokenizerUtil.getFrequency(sb.toString());
        List<Map.Entry<String, Double>> tflist = new LinkedList<>();
        tflist.addAll(tfs.entrySet());
        Collections.sort(tflist, (o1, o2) -> {
            if(o1.getValue() > o2.getValue()) return -1;
            else if(o1.getValue() < o2.getValue()) return 1;
            else return 0;
        });

        for(Map.Entry<String, Double> tf : tflist) {
            System.out.println(tf.getKey() + " : " + tf.getValue());
        }
    }

}
