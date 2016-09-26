package com.weibo.ml.lda;

import com.weibo.io.GzipTextFileReader;
import com.weibo.io.TextFileReader;
import com.weibo.tool.StringUtil;
import sun.jvm.hotspot.debugger.win32.coff.Characteristics;

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
    protected Hashtable<String, int[]> nwz = new Hashtable<String, int[]>();
    protected int[] topicSum;
    protected int totalSum;
    protected int numTopics;
    protected double alpha;
    protected double beta;
    protected int n;
    protected String[] explainations;
    protected ArrayList<String> words = new ArrayList<String>();

    protected int[] ndz;
    protected Random random = new Random();

    public static double LOG_MIN_PROB = -7;

    public double[] inference(String[] words) {
        double[] dist = new double[numTopics];
        inference(words, dist, 30, 10);
        return dist;
    }

    private void inference(String[] words, double[] pz, int numBurnInIterations, int numSamplingIterations) {
        words = removeUnknownWords(words);
        int[] z = new int[words.length];

        for (int i = 0; i < numTopics; i++) {
            ndz[i] = 0;
        }
        for (int i = 0; i < words.length; i++) {
            z[i] = random.nextInt(numTopics);
            ndz[z[i]]++;
        }


    }

    private String[] removeUnknownWords(String[] words) {
        ArrayList<String>  features = new ArrayList<String>();
        for (String word : words) {
            if (nwz.contains(word)) {
                features.add(word);
            }
        }
        return features.toArray(new String[0]);
    }

    //模拟随机选择主题
    protected int sampleInDistribution(double[] dist) {
        double p = random.nextDouble();
        double sum = 0;
        for (int i = 0 ; i < dist.length; i++) {
            sum += dist[i];
            if (sum >= p) {
                return i;
            }
        }
        return dist.length - 1;
    }

    public String explain(int topic, int top_n) {
        if (explainations[topic] == null) {
            String[] words = new String[top_n];
            double[] values = new double[top_n];
            int[] counts;
            for (Map.Entry<String, int[]> entry : nwz.entrySet()) {
                counts = entry.getValue();
                //词word_n对topic_n的贡献 / 所有词对topic_n的贡献
                double pwz = (counts[topic] + n * beta) / (topicSum[topic] + n * beta * nwz.size());
                //词word_n对该topic_n的贡献 / 词word_n对所有topic的贡献， 越大越好
                double pzw = (counts[topic] + n * beta) / (counts[numTopics] + n * beta * numTopics);
                double characteristic = Math.log(pwz + 1.0) * pzw;

                //取top_n
                for (int i = 0; i < top_n; i++) {
                    if (characteristic > values[i]) {
                        for (int m = top_n - 2; m >= i && m >= 0; m--) {
                            if (values[m] == 0.0) {
                                continue;
                            }
                            words[m+1] = words[m];
                            values[m+1] = values[m];
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
            assert(cols.length == numTopics + 1);
            int[] counts = new int[numTopics + 1];
            int sum = 0;
            for (int i = 0; i < numTopics; i++) {
                counts[i] = Integer.parseInt(cols[i+1]);
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
                " iterations:" + n);

        explainations = new String[numTopics];
        ndz = new int[numTopics];
    }

    /**
     * 某一主题在所有主题中占比程度
     * @param i
     * @return 返回值：某一主题总次数 / 所有主题总次数
     */
    public double pz(int topic) {
        return topicSum[topic] / totalSum;
    }

    public int getNumTopics() {
        return numTopics;
    }

    public int getNumWords() {
        return words.size();
    }

    public String getWord(int i) {
        return words.get(i);
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
