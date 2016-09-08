package com.weibo.ml.lda;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yuanye on 2016/9/8.
 */
public class GibbsSamplingReducer implements Reducer<Text, DocumentWritable, Text, DocumentWritable> {

    public void configure(JobConf jobConf) {

    }

    public void reduce(Text text, Iterator<DocumentWritable> iterator, OutputCollector<Text, DocumentWritable> outputCollector, Reporter reporter) throws IOException {

    }

    public void close() throws IOException {

    }
}
