package com.weibo.test;

import com.weibo.ml.lda.LdaModel;
import com.weibo.tool.StringUtil;

import java.io.IOException;
import java.util.*;

/**
 * Created by yuanye8 on 16/9/27.
 */
public class TestInference {
    public static void main(String[] args) throws IOException {
        LdaModel lda = new LdaModel();
        lda.loadModel("/Users/yuanye/Downloads/hadoop_lda_model");

        String a = "税前 扣除 明白 税前 意思 并入 工资 薪金 所得 缴纳 个人所得税 税前 扣除";
        String b = "注定 伟大 加内特 传奇 巨星 退役 传奇 终结";
        String[] test1 = a.split(" ");
        for (int i = 0; i < 100; i++) {
            inferenceDoc(lda, test1);
        }

    }

    public static void printProb(double[] p) {
        Hashtable<Integer, Double> hash = new Hashtable<Integer, Double>();
        for (int i = 0; i < p.length; i++) {
            hash.put(i, p[i]);
        }
        Set set = hash.entrySet();
        Map.Entry<Integer, Double>[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);
        Arrays.sort(entries, new Comparator() {
            public int compare(Object o1, Object o2) {
                double v1 =  (Double) ((Map.Entry)o1).getValue();
                double v2 =  (Double) ((Map.Entry)o2).getValue();
                return Double.compare(v2, v1);
            }
        });
        for (int i = 0; i < 5 && i < entries.length; i++) {
            int k = entries[i].getKey();
            double v = entries[i].getValue();
            System.out.print(k + " ");
        }
        System.out.println();
    }

    public static void inferenceDoc(LdaModel lda, String[] ss) {
        double[] p = lda.inference(ss);
        System.out.println("inference：" + StringUtil.join(ss, " "));
        printProb(p);
    }

    public static void inferenceDocFast(LdaModel lda, String[] ss) {
        double[] p = lda.inferenceFast(ss);
        System.out.println("inference fast：" + StringUtil.join(ss, " "));
        printProb(p);
    }

}
