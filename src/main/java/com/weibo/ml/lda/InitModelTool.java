package com.weibo.ml.lda;

import com.weibo.mapred.MapReduceJobConf;
import com.weibo.misc.AnyDoublePair;
import com.weibo.misc.Flags;
import com.weibo.tool.FolderReader;
import com.weibo.tool.FolderWriter;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * LDA模型第一次运行前的初始化操作（及预操作）
 * Created by yuanye8 on 16/9/2.
 */
public class InitModelTool implements GenericTool {

    private static Logger LOG = Logger.getAnonymousLogger();

    public void run(String[] args) throws Exception {
        Flags flags = new Flags();
        flags.add("input");
        flags.add("output_docs");
        flags.add("output_nwz");
        flags.add("num_topics");
        flags.add("wordlist");
        flags.add("max_num_words");
        flags.add("min_df");
        flags.parseAndCheck(args);

        Path input = new Path(flags.getString("input"));
        Path tfdf = new Path(flags.getString("wordlist") + ".tf_df");
        Path wordlist = new Path(flags.getString("wordlist"));
        int maxNumWords = flags.getInt("max_num_words");
        int minDf = flags.getInt("min_df");

        makeWordList(input, tfdf);
        int numWords = selectWords(tfdf, wordlist, maxNumWords, minDf);

        initModel(input, new Path(flags.getString("output_docs")), new Path(flags.getString("output_nwz")),
                wordlist, flags.getInt("num_topics"), numWords);

    }

    /**
     * 处理结果均已文本格式写在HDFS上，可以直接使用hadoop cat命令查看
     * @param tfdf  输入：词频文件路径
     * @param wordlist  输出：词权重，格式为 "word weight"
     * @param maxNumWords 输入：设定的允许最大词数
     * @param minDf 输入：设定的某个词存在于各文档中的文档的个数，用于过滤低质量词
     * @return 所有语料的词汇集合的大小
     * @throws IOException
     */
    public int selectWords(Path tfdf, Path wordlist, int maxNumWords, int minDf) throws IOException {
        Map<String, WordFreq> wordCounts = loadWordFreq(tfdf);
        List<String> specialKeys = new LinkedList<String>();
        WordFreq total = (WordFreq) wordCounts.get(WordListMapper.NUM_DOCS_STRING);
        if (total == null) {
            throw new RuntimeException("No number of docs key in the word list.");
        }

        List<AnyDoublePair<String>> weights = new ArrayList<AnyDoublePair<String>>();
        //特殊词汇含义不清，可能为遗留问题
        for (Map.Entry<String, WordFreq> e : wordCounts.entrySet()) {
            if (e.getKey().startsWith("_")) {
                specialKeys.add(e.getKey());
            } else if (!e.getKey().equals(WordListMapper.NUM_DOCS_STRING)) {
                WordFreq wf = e.getValue();
                if (wf.df > minDf) {
                    /**
                     * 词权重计算公式：wf.tf / total.tf * Math.log(total.df / wf.df)
                     * 一个词出现的次数越多，且属于的文档越少，表明这个词区分度越大,因此具有的价值越大，权值就越高
                     */
                    double weight = wf.tf / total.tf * Math.log(total.df / wf.df);
                    weights.add(new AnyDoublePair<String>(e.getKey(), weight));
                }
            }
        }
        Collections.sort(weights, new Comparator<AnyDoublePair<String>>() {
            public int compare(AnyDoublePair<String> o1, AnyDoublePair<String> o2) {
                /**
                 * 若o1在前，为升序排列
                 * 若o2在前，为降序排列
                 */
                return Double.compare(o2.second, o1.second);
            }
        });
        FolderWriter writer = new FolderWriter(wordlist, Text.class, IntWritable.class);

        Text key = new Text();
        IntWritable value = new IntWritable();
        if (maxNumWords == -1) {
            maxNumWords = Integer.MAX_VALUE;
        }
        int numWords = Math.min(maxNumWords, weights.size());
        for (int i = 0; i < numWords; i++) {
            key.set(weights.get(i).first);
            //wordlist即wordmap的二进制文件，占用空间小，读取速度快
            value.set(i);
            writer.append(key, value);
        }
        for (String specialKey : specialKeys) {
            key.set(specialKey);
            value.set(numWords);
            writer.append(key, value);
            numWords++;
        }
        writer.close();
        //输出wordmap文件，非sequence文件，可以直接查看，方便调试
        JobConf envConf = new JobConf();
        FileSystem fs = FileSystem.newInstance(envConf);
        FSDataOutputStream out = fs.create(new Path(wordlist.getParent(), "wordmap.txt"));
        byte[] numberWriter = (String.valueOf(numWords) + "\n").getBytes();
        out.write(numberWriter, 0, numberWriter.length);
        // ??? 若存在"_"开头的特殊key，那么此处numWords数会大于weights的size，导致获取异常
        for (int i = 0; i< numWords; i++) {
            byte[] toWrite = (weights.get(i).first + "\t" + i + "\n").getBytes();
            out.write(toWrite, 0, toWrite.length);
        }
        out.close();
        fs.close();

        LOG.info("Load " + wordCounts.size() + " words, keep " + numWords);
        return numWords;
    }

    /**
     * 读取计算后的词频文件
     * @param sqfile
     * @return
     * @throws IOException
     */
    public Map<String, WordFreq> loadWordFreq(Path sqfile) throws IOException {
        Hashtable<String, WordFreq> keymap = new Hashtable<String, WordFreq>();
        FolderReader reader = new FolderReader(sqfile);
        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
            WordFreq wf = new WordFreq();
            String str = value.toString();
            int split = str.indexOf(' ');
            wf.tf = Long.parseLong(str.substring(0, split));
            wf.df = Long.parseLong(str.substring(split + 1));
            keymap.put(key.toString(), wf);
        }
        reader.close();
        return keymap;
    }

    /**
     * 词频和文档频率统计
     * @param input
     * @param output
     * @throws IOException
     */
    public void makeWordList(Path input, Path output) throws IOException {
        MapReduceJobConf job = new MapReduceJobConf();
        job.setJobName("EstimateWordFreqForLDA");
        job.setMapReduce(WordListMapper.class, WordListReducer.class);
        job.setCombinerClass(WordListCombiner.class);
        job.setKeyValueClass(Text.class, Text.class, Text.class, Text.class);
        job.setInputOutputPath(input, output);
        JobClient.runJob(job);
    }

    public void initModel(Path input, Path outputDocs, Path outputNwz, Path wordlist, int numTopics, int numWords)
        throws IOException
    {
        JobConf envConf = new JobConf();
        FileSystem fs = FileSystem.get(envConf);

        //确保路径下的权限
        Path tmpNwz = new Path(outputNwz, "_tmp").makeQualified(fs);
        wordlist = wordlist.makeQualified(fs);

        MapReduceJobConf job = new MapReduceJobConf(getClass());
        // ??? fs.mkdirs(tmpNwz);
        FileSystem.get(job).mkdirs(tmpNwz);
        job.setJobName("InitializeModelForLDA");
        job.setMapReduce(InitModelMapper.class, InitModelReducer.class);
        job.setKeyValueClass(Text.class, DocumentWritable.class, Text.class, DocumentWritable.class);

        job.setInputOutputPath(input, outputDocs);

        job.set("wordlist", wordlist.toString());
        job.set("output.nwz", tmpNwz.toString());
        job.setInt("num.topics", numTopics);
        job.setInt("num.words", numWords);
        JobClient.runJob(job);

        combineModelParm(tmpNwz, outputNwz);
        fs.delete(tmpNwz);
        System.out.println("Done");
    }

    private void combineModelParm(Path tmpNwz, Path outputNwz) throws IOException {
        MapReduceJobConf job = new MapReduceJobConf(getClass());
        job.setJobName("CombineModelParametersForLDA");
        job.setInputOutputPath(tmpNwz, outputNwz);
        job.setMapReduce(IdentityMapper.class, CombineModelParamReducer.class);
        job.setKeyValueClass(IntWritable.class, WordInfoWritable.class, IntWritable.class, WordInfoWritable.class);

        job.setBoolean("take.mean", false);
        JobClient.runJob(job);
    }

    private static class WordFreq {
        public double tf;
        public double df;
    }
}
