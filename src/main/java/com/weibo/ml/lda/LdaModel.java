package com.weibo.ml.lda;

import com.weibo.io.GzipTextFileReader;
import com.weibo.io.TextFileReader;
import com.weibo.tool.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Load a trained LDA model, do various inferences about it.
 * Created by yuanye8 on 16/9/26.
 */
public class LdaModel {

    protected static Logger LOG = Logger.getLogger(LdaModel.class.getName());

    /**
     * 训练模型导入，int数组加了一列，为前K列的总和，即某个词共被分配到K个主题下的总次数
     */
    protected Hashtable<String, int[]> nwz = new Hashtable<String, int[]>();

    /**
     * 所有词被分配到K个主题下的次数分布
     */
    protected int[] topicSum;

    /**
     * K个主题下的次数分布的总和，相当于topicSum所有元素之和
     */
    protected int totalSum;

    protected int numTopics;

    protected double alpha;

    protected double beta;

    /**
     * 相当于iterations_to_keep
     */
    protected int n;

    /**
     * 描述K个主题，每个数组元素是一串n个词组成的字符串
     */
    protected String[] explainations;

    protected ArrayList<String> words = new ArrayList<String>();

    /**
     * 文档被分配到n个主题下的次数，长度K，即numTopics
     */
    protected int[] ndz;
    protected Random random = new Random();

    public static double LOG_MIN_PROB = -7;

    public double[] inference(String[] words) {
        double[] dist = new double[numTopics];
        //30,10
        inference(words, dist, 30, 10);
        return dist;
    }

    /**
     * inference method one, long time consuming
     * remove no-relation words first， not concern every words effection
     *
     * @param words                 文档切词后输入
     * @param pz                    文档主题概率分布
     * @param numBurnInIterations
     * @param numSamplingIterations
     */
    private void inference(String[] words, double[] pz, int numBurnInIterations, int numSamplingIterations) {
        words = removeUnknownWords(words);
        //每个词的主题
        int[] z = new int[words.length];

        //初始化每个文档主题分布 及 随机初始化每个词的主题
        for (int i = 0; i < numTopics; i++) {
            ndz[i] = 0;
        }
        for (int i = 0; i < words.length; i++) {
            z[i] = random.nextInt(numTopics);
            ndz[z[i]]++;
        }

        // Burn-in.
        for (int i = 0; i < numBurnInIterations; i++) {
            for (int j = 0; j < words.length; j++) {
                int oldTopic = z[j];
                --ndz[oldTopic];
                calculateConditionalProbability(words[j], ndz, pz, words.length);
                int newTopic = sampleInDistribution(pz);
                z[j] = newTopic;
                ++ndz[newTopic];
            }
        }

        // Inference.
        for (int i = 0; i < numSamplingIterations; i++) {
            for (int j = 0; j < words.length; j++) {
                calculateConditionalProbability(words[j], ndz, pz, words.length);
                int newTopic = sampleInDistribution(pz);
                z[j] = newTopic;
                ++ndz[newTopic];
            }
        }

        double norm = 0.0;
        for (int i = 0; i < pz.length; i++) {
            pz[i] = ndz[i] + alpha;
            norm += pz[i];
        }
        for (int i = 0; i < pz.length; i++) {
            pz[i] /= norm;
        }

    }

    /**
     * 计算新来文档的其中一个词在各主题下的概率分布
     *
     * @param word
     * @param ndz
     * @param pz
     * @param doclength
     */
    private void calculateConditionalProbability(String word, int[] ndz, double[] pz, int doclength) {
        int[] counts = nwz.get(word);
        double sum = 0.0;
        double normalizer = (doclength + numTopics * alpha - 1);
        for (int i = 0; i < numTopics; i++) {
            pz[i] = (counts[i] + beta) / (ndz[i] + nwz.size() * beta - 1) * (ndz[i] + alpha) / normalizer;
            sum += pz[i];
        }
        for (int i = 0; i < numTopics; i++) {
            pz[i] /= sum;
        }
    }

    public double[] inferenceFast(String[] doc) {
        double[] p = new double[numTopics];
        inferenceFast(doc, p);
        return p;
    }

    /**
     * inference method two, execute fast
     * concern every words effection
     *
     * @param doc
     * @param p
     */
    private void inferenceFast(String[] doc, double[] p) {
        for (int i = 0; i < numTopics; i++) {
            p[i] = 0.0;
        }
        for (int i = 0; i < doc.length; i++) {
            //遍历每个单词，计算每个单词对每个主题的贡献
            int[] counts = nwz.get(doc[i]);
            for (int k = 0; k < numTopics; k++) {
                if (counts == null) {
                    p[k] += LdaModel.LOG_MIN_PROB;
                } else {
                    p[k] += Math.log((counts[k] + n * beta) / (topicSum[k] + n * beta * nwz.size()));
                }
            }
        }
        double norm = 0.0;
        for (int i = 0; i < numTopics; i++) {
            norm += Math.exp(p[i]);
        }
        for (int i = 0; i < numTopics; i++) {
            p[i] = Math.exp(p[i]) / norm;
        }
    }

    /**
     * Only keep words in vocabulary for inference.
     *
     * @param words
     * @return
     */
    private String[] removeUnknownWords(String[] words) {
        ArrayList<String> features = new ArrayList<String>();
        for (String word : words) {
            //contains() are same like containsValue()
            if (nwz.containsKey(word)) {
                features.add(word);
            }
        }
        return features.toArray(new String[0]);
    }

    /**
     * Mock random choose topic.
     *
     * @param dist
     * @return
     */
    protected int sampleInDistribution(double[] dist) {
        double p = random.nextDouble();
        double sum = 0;
        for (int i = 0; i < dist.length; i++) {
            sum += dist[i];
            if (sum >= p) {
                return i;
            }
        }
        return dist.length - 1;
    }

    /**
     * Describe certain "topic n" with words of specified number most likely under it.
     *
     * @param topic
     * @param top_n max number of words to describe a topic
     * @return
     */
    public String explain(int topic, int top_n) {
        if (explainations[topic] == null) {
            String[] words = new String[top_n];
            double[] values = new double[top_n];
            int[] counts;
            for (Map.Entry<String, int[]> entry : nwz.entrySet()) {
                counts = entry.getValue();
                //词word_n对topic_n的贡献 / 所有词对topic_n的贡献
                double pwz = (counts[topic] + n * beta) / (topicSum[topic] + n * beta * nwz.size());
                //词word_n对该topic_n的贡献 / 词word_n对所有topic的贡献
                double pzw = (counts[topic] + n * beta) / (counts[numTopics] + n * beta * numTopics);
                //最终评价值：值越大，表示该词在此topic下的可能性越大
                double characteristic = Math.log(pwz + 1.0) * pzw;

                //取top_n, 采用插入排序法
                for (int i = 0; i < top_n; i++) {
                    if (characteristic > values[i]) {
                        for (int m = top_n - 2; m >= i && m >= 0; m--) {
                            if (values[m] == 0.0) {
                                continue;
                            }
                            words[m + 1] = words[m];
                            values[m + 1] = values[m];
                        }
                        words[i] = entry.getKey();
                        values[i] = characteristic;
                        break;
                    }
                }
            }
            explainations[topic] = StringUtil.join(words, " ");
        }
        return explainations[topic];
    }

    /**
     * Loading nwz file, calculate total number of
     * each word assigned to certain topic and total
     * number of all words assigned to topics.
     *
     * @param model
     * @throws IOException
     */
    public void loadModel(String model) throws IOException {
        nwz.clear();
        TextFileReader reader = null;
        if (model.endsWith(".gz")) {
            reader = new GzipTextFileReader(model, "UTF-8");
        } else {
            reader = new TextFileReader(model, "UTF-8");
        }

        alpha = Double.parseDouble(reader.readLine());
        beta = Double.parseDouble(reader.readLine());
        numTopics = Integer.parseInt(reader.readLine());
        n = Integer.parseInt(reader.readLine());
        topicSum = new int[numTopics];
        totalSum = 0;

        String line;
        while ((line = reader.readLine()) != null) {
            String[] cols = line.split(" ");
            assert (cols.length == numTopics + 1);
            int[] counts = new int[numTopics + 1];
            int sum = 0;
            for (int i = 0; i < numTopics; i++) {
                counts[i] = Integer.parseInt(cols[i + 1]);
                topicSum[i] += counts[i];
                sum += counts[i];
            }
            //number of certain word assigned to topics
            counts[numTopics] = sum;
            totalSum += sum;
            nwz.put(cols[0], counts);
            words.add(cols[0]);
        }
        reader.close();

        LOG.info("Load model parameters, alpha:" + alpha + " beta:" + beta +
                " num_topics:" + numTopics + " num_words:" + nwz.size() +
                " iterations:" + n + " totalSum:" + totalSum);

        explainations = new String[numTopics];
        ndz = new int[numTopics];
    }

    /**
     * 某一主题在所有主题中占比程度
     *
     * @param topic
     * @return 返回值：单词背分到某一主题的总次数 / 单词被分到所有主题的总次数
     */
    public double pz(int topic) {
        return topicSum[topic] / (double) totalSum;
    }

    public String getWord(int i) {
        return words.get(i);
    }

    public int getNumTopics() {
        return numTopics;
    }

    public int getNumWords() {
        return words.size();
    }

    public int getNumTrainingIterations() {
        return n;
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

}
