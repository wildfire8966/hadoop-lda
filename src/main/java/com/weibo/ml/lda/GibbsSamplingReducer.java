package com.weibo.ml.lda;

import com.weibo.tool.FolderReader;
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
import java.util.logging.Logger;

/**
 * LDA核心迭代部分，吉布斯三重采样： -1 -> sampling -> +1
 * 相当于去除某个词后，计算主题相关的概率（即某个词被重新分配）， 之后把该词的重新分配结果回填
 * Perform Gibbs Sampling on documents. When starting, the reducer loads p(w|z)
 * from the model file. Then it uses p(w|z) to sample topics for input documents
 * After all entries reduced, the reducer output the modified p(w|z) back to the
 * file system.
 * Created by yuanye on 2016/9/8.
 */
public class GibbsSamplingReducer implements Reducer<Text, DocumentWritable, Text, DocumentWritable> {
    public static Logger LOG = Logger.getAnonymousLogger();
    private int numTopics = 0;
    private double[] probs = null;
    private int[][] nwz = null;
    private int[] nzd = null;
    private int[] nz = null;
    private Random randomProvider = new Random();
    private double alpha = 0.0;
    private double beta = 0.0;
    private String outputNwz = null;
    private int numWords = 0;

    /**
     * 用于记录nwz的改变
     * 1. 当文档分布式的进行处理时，每个reducer都会维护一份全局的nwz
     * 2. 多份文档同时包含一个word但却分在不同reducer处理时，每个word都会
     * 在其reducer上进行重新主题分配，此时它维护的一份全局的nwz得到的改变不
     * 是全局的，而是部分的
     * 3. 因此设置delta_nwz，存储一个word部分的改变量，reducer结束后将其
     * 写入磁盘，再通过一个MapReduce任务，将所有的改变量汇总叠加，得到一个
     * word最后的改变量，即新的nwz
     */
    private int[][] delta_nwz = null;

    public void configure(JobConf conf) {
        //主题数 K
        this.numTopics = conf.getInt("num.topics", 0);
        //词规模 V
        this.numWords = conf.getInt("num.words", 0);
        //吉布斯采样使用，词被分到每个主题的概率
        this.probs = new double[this.numTopics];
        //某个文档被分到K个主题下的次数
        this.nzd = new int[this.numTopics];
        //某个主题得到的分配到其下的词的数目
        this.nz = new int[this.numTopics];
        this.outputNwz = conf.get("output.nwz");
        this.alpha = (double) conf.getFloat("alpha", 0.0F);
        this.beta = (double) conf.getFloat("beta", 0.0F);
        try {
            loadModelParameters(conf.get("input.nwz"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取上一次存储的模型数据
     * @param modelParamFile nwz数据，即word->topic（phi矩阵）
     * @throws IOException
     */
    private void loadModelParameters(String modelParamFile) throws IOException {
        long startTime = System.currentTimeMillis();
        Path modelParamPath = new Path(modelParamFile);
        FolderReader fr = new FolderReader(modelParamPath);
        IntWritable key = new IntWritable();
        WordInfoWritable value = new WordInfoWritable(this.numTopics);
        Arrays.fill(this.nz, 0);

        this.nwz = new int[this.numWords][];
        this.delta_nwz = new int[this.numWords][];

        while (fr.next(key, value)) {
            int[] count = new int[this.numTopics];
            for (int i = 0; i < this.numTopics; i++) {
                count[i] = value.getTopicCount(i);
                this.nz[i] += count[i];
            }
            this.nwz[key.get()] = count;
        }

        for (int i = 0; i < this.numWords; i++) {
            if (this.nwz[i] == null)  {
                this.nwz[i] = new int[this.numTopics];
            }
            if (this.delta_nwz[i] == null) {
                this.delta_nwz[i] = new int[this.numTopics];
            }
            Arrays.fill(this.delta_nwz[i], 0);
        }
        fr.close();
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Load model parameters using " + duration + " milliseconds.");
    }

    public void reduce(Text key, Iterator<DocumentWritable> values, OutputCollector<Text, DocumentWritable> outputCollector, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            DocumentWritable doc = values.next();
            computeNzd(doc, this.nzd);
            double likelihood = 0.0;
            int doc_length = doc.getNumWords();

            for (int i = 0; i < doc.getNumWords(); i++) {
                int topic = doc.topics[i];
                int word = doc.words[i];

                this.nzd[topic]--;
                this.nz[topic]--;
                this.nwz[word][topic]--;
                delta_nwz[word][topic]--;

                likelihood += computeSamplingProbability(this.nzd, word, this.probs, this.alpha, this.beta, doc_length - 1);
                topic = sampleInDistribution(this.probs, this.randomProvider);

                doc.topics[i] = topic;
                this.nzd[topic]++;
                this.nz[topic]++;
                this.nwz[word][topic]++;
                this.delta_nwz[word][topic]++;

            }
            /**
             * likelihood值可以调节
             * likelihood * GibbsSamplingTool.RESOLUTION 使得值过小，因此直接增加likelihood
             */
            reporter.incrCounter(
                    GibbsSamplingTool.GibbsSamplingCounter.LIKELIHOOD,
                    (long) likelihood);
            outputCollector.collect(key, doc);
        }
    }

    /**
     * 模拟随机抽样，随机抽取一个主题
     * @param probs
     * @param randomProvider
     * @return
     */
    private int sampleInDistribution(double[] probs, Random randomProvider) {
        double sample = randomProvider.nextDouble();
        double sum = 0.0D;
        for (int i = 0; i < probs.length; i++) {
            sum += probs[i];
            if (sample < sum) {
                return i;
            }
        }
        return probs.length - 1;
    }

    private double computeSamplingProbability(int[] nzd, int word, double[] probs, double alpha, double beta, int doc_length) {
        Arrays.fill(probs, 0.0);
        double norm = 0.0;
        //遗留代码，之前替换pzd的分母
        double dummyNorm = 1.0;
        double likelihood = 0.0;
        for (int i = 0; i < this.numTopics; i++) {
            //word这个词在第i个topic下的概率
            double pwz = (this.nwz[word][i] + beta) / (this.nz[i] + this.nwz.length * beta);
            //第i个主题在该文档下的概率
            double pzd = (nzd[i] + alpha) / (doc_length + this.numTopics * alpha);
            //该文档选中该主题的概率
            probs[i] = (pwz * pzd);
            norm += probs[i];
            likelihood += pwz;
        }
        for (int i = 0; i < this.numTopics; i++) {
            probs[i] /= norm;
        }
        return likelihood;
    }

    /**
     * 计算当前文档的主题分布
     * 方法为统计该文档的每个词被分到的主题，累加后得到该文档的主题分布
     * @param doc 当前文档
     * @param nzd 当前文档的主题分布
     */
    public void computeNzd(DocumentWritable doc, int[] nzd) {
        Arrays.fill(nzd, 0);
        for (int i = 0; i < doc.getNumWords(); i++) {
            nzd[doc.topics[i]]++;
        }
    }

    public void saveModelParameters(String modelParamPart) throws IOException {
        long startTime = System.currentTimeMillis();
        JobConf envConf = new JobConf();
        SequenceFile.Writer writer = SequenceFile.createWriter(
                FileSystem.get(envConf),
                envConf,
                new Path(modelParamPart),
                IntWritable.class,
                WordInfoWritable.class
        );
        IntWritable key = new IntWritable();
        WordInfoWritable value = new WordInfoWritable(numTopics);
        for (int i = 0; i < nwz.length; i++) {
            key.set(i);
            for (int j = 0; j < numTopics; j++) {
                value.setTopicCount(j, delta_nwz[i][j]);
            }
            value.setIsPartial(true);
            writer.append(key, value);
        }
        writer.close();
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Save model parameters using " + duration + " milliseconds.");
    }

    public void close() throws IOException {
        //此处的nextInt是个伪随机数
        String partName = "part-" + Math.abs(randomProvider.nextInt());
        saveModelParameters(outputNwz + "/" + partName);
    }
}
