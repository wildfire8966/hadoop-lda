package com.weibo.ml.lda;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * 词频、文档频率统计的Redcue程序
 * 输出：
 * word tf df 单个词在所有文档中出现的次数，和出现在所有文档中独立出现的次数（不算重复出现）
 * " " tf df 所有文档出现的词的总数，和文档总数
 * Created by yuanye8 on 16/9/6.
 */
public class WordListReducer implements Reducer<Text, Text, Text, Text> {
    Text outvalue = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        long tf = 0L;
        long df = 0L;
        while (values.hasNext()) {
            String value = values.next().toString();
            if (value.charAt(0) == 'd') {
                df += Long.parseLong(value.substring(1));
            } else if (value.charAt(0) == 't') {
                tf += Long.parseLong(value.substring(1));
            }
        }
        this.outvalue.set(tf + " " + df);
        outputCollector.collect(key, this.outvalue);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
