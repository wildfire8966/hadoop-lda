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
 * Created by yuanye8 on 16/9/7.
 */
public class CombineModelParamReducer implements Reducer<IntWritable, WordInfoWritable, IntWritable, WordInfoWritable> {
    private int[] topicCount = null;
    //此处无实际用处，原代码遗留
    private int[] referenceCount = null;
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
                for (int i = 0; i < v.size(); i++) {
                    this.topicCount[i] += v.getTopicCount(i);
                }
            } else {
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
