package com.weibo.ml.lda;

import com.weibo.tool.FolderReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
    private double alpha = 0.0D;
    private double beta = 0.0D;
    private String outputNwz = null;
    private int numWords = 0;

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
        this.alpha = conf.getFloat("alpha", 0.0F);
        this.beta = conf.getFloat("beta", 0.0F);
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
            //long begin = System.nanoTime();
            DocumentWritable doc = (DocumentWritable) values.next();
            computeNzd(doc, this.nzd);
            double likelihood = 0.0;
            int doc_length = doc.getNumWords();

            for (int i = 0; i < doc.getNumWords(); i++) {
                int topic = doc.topics[i];
                int word = doc.words[i];

                this.nzd[topic] -= 1;
                this.nz[topic] -= 1;
                this.nwz[word][topic] -= 1;
                delta_nwz[word][topic] -= 1;

                likelihood += computeSamplingProbability(this.nzd, word, this.probs, this.alpha, this.beta, doc_length - 1);
                topic = sampleInDistribution(this.probs, this.randomProvider);

                doc.topics[i] = topic;
                this.nzd[topic] += 1;
                this.nz[topic] += 1;
                this.nwz[word][topic] += 1;
                this.delta_nwz[word][topic] += 1;

            }
            //long end1 = System.nanoTime();
            //LOG.info("1: " + (end1 - begin));
            /**
             * 此处乘以100，是为了使likelihood得到一个非零数字
             * 按原来代码的话，强转成long后永远都是0，likelihood失去意义
             */
            reporter.incrCounter(
                    GibbsSamplingTool.GibbsSamplingCounter.LIKELIHOOD,
                    (long) (likelihood /* / doc.getNumWords() */ * GibbsSamplingTool.RESOLUTION * 100));
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
        Arrays.fill(probs, 0.0D);
        double norm = 0.0D;
        double dummyNorm = 1.0D;
        double likelihood = 0.0D;
        for (int i = 0; i < this.numTopics; i++) {
            //word这个词在第i个topic下的概率
            double pwz = (this.nwz[word][i] + beta) / (this.nz[i] + this.nwz.length * beta);
            //第i个主题在改文档下的概率
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
            nzd[doc.topics[i]] += 1;
        }
    }

    public void close() throws IOException {

    }
}
