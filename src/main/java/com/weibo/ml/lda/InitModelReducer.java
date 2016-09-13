package com.weibo.ml.lda;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * nwz二维数组记录了每个word被分在不同topic下的次数
 * Created by yuanye8 on 16/9/6.
 */
public class InitModelReducer implements Reducer<Text, DocumentWritable, Text, DocumentWritable> {
    int numTopics = 0;
    int numWords = 0;
    int[][] nwz = null;
    String outputNwz = null;
    Random randomProvider = new Random();

    public void configure(JobConf jobConf) {
        this.numTopics = jobConf.getInt("num.topics", 0);
        this.numWords = jobConf.getInt("num.words", 0);
        this.outputNwz = jobConf.get("output.nwz");
        this.nwz = new int[this.numWords][];
    }

    //输出 id : doc(已初始化，int[] words, int[] topics)
    public void reduce(Text key, Iterator<DocumentWritable> values, OutputCollector<Text, DocumentWritable> outputCollector, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            DocumentWritable doc = (DocumentWritable) values.next();

            for (int i = 0; i < doc.getNumWords(); i++) {
                int word = doc.words[i];
                int topic = this.randomProvider.nextInt(this.numTopics);
                doc.topics[i] = topic;
                int[] counts = this.nwz[word];
                if (counts == null) {
                    counts = new int[this.numTopics];
                    counts[topic] += 1;
                    this.nwz[word] = counts;
                } else {
                    counts[topic] += 1;
                }
            }
            outputCollector.collect(key, doc);
        }
    }

    public void close() throws IOException {
        String partName = "part-" + Math.abs(this.randomProvider.nextInt());
        JobConf envConf = new JobConf();
        SequenceFile.Writer writer = SequenceFile.createWriter(
                FileSystem.get(envConf),
                envConf,
                new Path(this.outputNwz + "/" + partName),
                IntWritable.class,
                WordInfoWritable.class);
        saveModelParameters(this.nwz, writer);
        writer.close();
    }

    private void saveModelParameters(int[][] nwz, SequenceFile.Writer writer) throws IOException {
        IntWritable key = new IntWritable();
        WordInfoWritable value = new WordInfoWritable(this.numTopics);
        int[] zeros = new int[this.numTopics];
        Arrays.fill(zeros, 0);
        for (int i = 0 ; i < nwz.length; i++) {
            key.set(i);
            int[] counts = nwz[i];
            if (counts == null) {
                counts = zeros;
            }
            for (int j = 0; j < this.numTopics; j++) {
                value.setTopicCount(j, counts[j]);
            }
            value.setIsPartial(true);
            writer.append(key, value);
        }
    }

}
