package com.weibo.ml.lda;

import com.weibo.misc.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * 计算词频和文档频率
 * tf ： term-frequency
 * df ： document-frequency
 * 输出
 * word d1 单个词的文档频率
 * word tn 单个词的词频率
 * " " d1 一篇文档贡献一次技术
 * " " tn 所有文档出现词的总数
 * Created by yuanye8 on 16/9/6.
 */
public class WordListMapper implements Mapper<Text, Text, Text, Text> {
    public static String NUM_DOCS_STRING = " ";
    Text outkey = new Text();
    Text outvalue = new Text();
    Counter<String> wordfreq = new Counter<String>();

    public void configure(JobConf jobConf) {

    }

    public void map(Text key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String[] words = value.toString().split(" ");
        this.wordfreq.clear();
        for (String word : words) {
            this.wordfreq.inc(word, 1L);
        }
        Iterator iter = this.wordfreq.iterator();
        long numWords = 0L;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            this.outkey.set((String) entry.getKey());
            this.outvalue.set("d1");
            //某个词独在某一文档出现1次，不重复
            outputCollector.collect(this.outkey, this.outvalue);
            this.outvalue.set("t" + entry.getValue());
            //某个词在某一档出现的总次数，重复
            outputCollector.collect(this.outkey, this.outvalue);
            numWords += ((Long) entry.getValue()).longValue();
        }
        this.outkey.set(NUM_DOCS_STRING);
        this.outvalue.set("d1");
        //某篇文档出现过1次
        outputCollector.collect(this.outkey, this.outvalue);
        this.outvalue.set("t" + numWords);
        //某篇文档的词的总数（词重复算多次）
        outputCollector.collect(this.outkey, this.outvalue);
    }

    public void close() throws IOException {

    }

}
