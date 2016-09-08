package com.weibo.ml.lda;

import com.weibo.tool.FolderReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * 初始化模型Mapper程序，将文档读入、切分、按词典转化为int数组，以自定输出类DocumentWritable进行输出
 * 初始化了word[], topic[], 两者为对应关系，长度一致相等，用于描述一个文档的情况:这个文档的特征词及特征词属于的主题
 * Created by yuanye8 on 16/9/6.
 */
public class InitModelMapper implements Mapper<Text, Text, Text, DocumentWritable> {
    private static Logger LOG = Logger.getAnonymousLogger();
    Map<String, Integer> wordmap = null;
    DocumentWritable doc = new DocumentWritable();
    List<Integer> wordbuf = new ArrayList<Integer>();

    public void map(Text key, Text value, OutputCollector<Text,
            DocumentWritable> outputCollector, Reporter reporter) throws IOException {
        //以单个或连续多个空格进行分割
        String[] words = value.toString().split(" +");
        this.wordbuf.clear();
        for (int i = 0; i < words.length; i++) {
            Integer id = (Integer)this.wordmap.get(words[i]);
            if (id != null) {
                this.wordbuf.add(id);
            }
        }
        //此处初始化了topic[]数组和words[]数组
        this.doc.setNumWords(this.wordbuf.size());
        for (int i = 0; i < this.wordbuf.size(); i++) {
            this.doc.words[i] = this.wordbuf.get(i).intValue();
        }
        outputCollector.collect(key, this.doc);
    }

    public void configure(JobConf jobConf) {
        try {
            this.wordmap = loadWordList(jobConf.get("wordlist"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setWordList(Hashtable<String, Integer> wordmap) {
        this.wordmap = wordmap;
    }

    /**
     * 读取wordmap文件并转化为hashtable对象
     * @param wordlist
     * @return
     * @throws IOException
     */
    private Map<String,Integer> loadWordList(String wordlist) throws IOException {
        Hashtable keymap = new Hashtable();
        FolderReader reader = new FolderReader(new Path(wordlist));
        Text key = new Text();
        IntWritable value = new IntWritable();
        while (reader.next(key, value)) {
            keymap.put(key.toString(), Integer.valueOf(value.get()));
        }
        reader.close();
        return keymap;
    }

    public void close() throws IOException {

    }
}
