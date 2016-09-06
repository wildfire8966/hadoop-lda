package com.weibo.ml.lda;

import com.weibo.mapred.MapReduceJobConf;
import com.weibo.misc.Flags;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * LDA模型第一次运行前的初始化操作（及预操作）
 * Created by yuanye8 on 16/9/2.
 */
public class InitModelTool implements GenericTool {

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

    }
}
