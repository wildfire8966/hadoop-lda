package com.weibo.ml.lda;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yuanye8 on 16/9/6.
 */
public class InitModelReducer implements Reducer {
    public void reduce(Object o, Iterator iterator, OutputCollector outputCollector, Reporter reporter) throws IOException {

    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
