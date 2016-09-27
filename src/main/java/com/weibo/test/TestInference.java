package com.weibo.test;

import com.weibo.ml.lda.LdaModel;
import com.weibo.tool.StringUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yuanye8 on 16/9/27.
 */
public class TestInference {
    public static void main(String[] args) throws IOException {
        LdaModel lda = new LdaModel();
        lda.loadModel("/Users/yuanye8/Downloads/CRT_UP_DOWN/hadoop_lda_model");

        String a = "税前 扣除 明白 税前 意思 并入 工资 薪金 所得 缴纳 个人所得税 税前 扣除";
        String[] test1 = a.split(" ");
        inferenceDoc(lda, test1);
    }

    public static void printProb(double[] p) {
        double max = 0.0;
        double index = 0;
        int i;
        for (i = 0; i < p.length; i++) {
            if (p[i] > max) {
                max = p[i];
                index = i;
            }
        }
        System.out.println(index);
    }

    public static void inferenceDoc(LdaModel lda, String[] ss) {
        double[] p1 = lda.inference(ss);
        //double[] p2 = lda.inferenceFast(ss);
        System.out.println(StringUtil.join(ss, " "));
        System.out.println("inference");
        printProb(p1);
        //System.out.println("inferenceFast");
        //printProb(p2);
    }

}
