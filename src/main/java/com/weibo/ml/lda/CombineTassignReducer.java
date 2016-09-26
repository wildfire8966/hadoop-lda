package com.weibo.ml.lda;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Ouput tassign matrix as text file for lda
 * every line in this text file represents a document.
 * the line of tassign matrix looks like:
 * word_id1:topic1 word_id2:topic2 ... word_id_n:topic_n
 * Created by yuanye8 on 16/9/12.
 */
public class CombineTassignReducer implements Reducer<Text, DocumentWritable, NullWritable, Text> {

    public void reduce(Text key, Iterator<DocumentWritable> values, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            DocumentWritable doc = values.next();
                StringBuffer sb = new StringBuffer();
                int[] words = doc.words;
                int[] topics = doc.topics;
                for (int i = 0; i < doc.getNumWords(); i++) {
                sb.append(words[i]).append(":").append(topics[i]).append(" ");
            }
            outputCollector.collect(NullWritable.get(), new Text(sb.toString()));
        }
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
