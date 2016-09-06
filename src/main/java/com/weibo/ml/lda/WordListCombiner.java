package com.weibo.ml.lda;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * 优化类：减少网络传输
 * Created by yuanye8 on 16/9/6.
 */
public class WordListCombiner implements Reducer<Text, Text, Text, Text> {
    Text outvalue = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        long tf = 0L;
        long df = 0L;
        while (values.hasNext()) {
            String value = (String) values.next().toString();
            if (value.charAt(0) == 'd') {
                df += Long.parseLong(value.substring(1));
            } else if (value.charAt(0) == 't') {
                tf += Long.parseLong(value.substring(1));
            }
        }
        this.outvalue.set("t" + tf);
        outputCollector.collect(key, this.outvalue);
        this.outvalue.set("d" + df);
        outputCollector.collect(key, this.outvalue);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }

}
