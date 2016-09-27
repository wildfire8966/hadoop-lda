package com.weibo.ml.lda;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Combine the word topic counts from different samplers.
 * referenceCount的由来详见GibbsSamplingReducer中delta_nwz的注释
 * Created by yuanye8 on 16/9/7.
 */
public class CombineModelParamReducer implements Reducer<IntWritable, WordInfoWritable, IntWritable, WordInfoWritable> {
    //用于记录本次计算的delta_nwz结果，是多个reducer的数据按key shuffle后的结果
    private int[] topicCount = null;
    //用于记录上次计算的nwz结果
    private int[] referenceCount = null;
    //用于输出本地迭代最终的nwz结果
    private WordInfoWritable outvalue = null;
    //暂无实际用处
    private boolean takeMean = true;

    public void configure(JobConf jobConf) {
        this.takeMean = jobConf.getBoolean("take.mean", true);
    }

    public void reduce(IntWritable key, Iterator<WordInfoWritable> values, OutputCollector<IntWritable, WordInfoWritable> outputCollector, Reporter reporter) throws IOException {
        int n = 0;
        while (values.hasNext()) {
            WordInfoWritable v = values.next();
            if (this.topicCount == null) {
                this.topicCount = new int[v.size()];
                this.referenceCount = new int[v.size()];
                this.outvalue = new WordInfoWritable(v.size());
            }
            if (n == 0) {
                Arrays.fill(this.topicCount, 0);
                Arrays.fill(this.referenceCount, 0);
            }
            if (v.isPartial) {
                //来自多个reducer的delta_nwz汇总
                for (int i = 0; i < v.size(); i++) {
                    this.topicCount[i] += v.getTopicCount(i);
                }
            } else {
                /**
                 * 来自上次计算的结果，第一次初始化数据时，referenceCount全部为0
                 */
                for (int i = 0; i < v.size(); i++) {
                    this.referenceCount[i] += v.getTopicCount(i);
                }
            }
            n++;
        }
        for (int i = 0; i< this.topicCount.length; i++) {
            this.outvalue.setTopicCount(i, this.topicCount[i] + this.referenceCount[i]);
        }
        //所有key下的部分合成为一个word的整体topics分布
        this.outvalue.setIsPartial(false);
        outputCollector.collect(key, this.outvalue);
    }

    public void close() throws IOException {
    }

}
