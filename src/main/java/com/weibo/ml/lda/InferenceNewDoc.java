package com.weibo.ml.lda;

import com.weibo.io.TextFileReader;
import com.weibo.io.TextFileWriter;
import com.weibo.misc.Flags;
import com.weibo.tool.GenericTool;

import java.text.DecimalFormat;
import java.util.*;

/**
 * 输入新文档计算top n主题并输出
 * input doc like: uid words cates
 * Created by yuanye8 on 16/9/28.
 */
public class InferenceNewDoc implements GenericTool {
    private TextFileReader reader = null;
    private TextFileWriter writer = null;
    private LdaModel ldaModel = null;
    private String inputPath = null;
    private String outputPath = null;
    private String modelPath = null;
    private int topN = 5;

    private ArrayList<String> inputList = new ArrayList<String>();

    public void run(String[] args) throws Exception {
        Flags flags = new Flags();
        flags.add("model");
        flags.add("input");
        flags.add("output");
        flags.add("top_n", "topics number of each doc");
        flags.parseAndCheck(args);

        inputPath = flags.getString("input");
        outputPath = flags.getString("output");
        modelPath = flags.getString("model");
        topN = flags.getInt("top_n");

        this.reader = new TextFileReader(inputPath);
        this.writer = new TextFileWriter(outputPath);
        this.ldaModel = new LdaModel();
        this.ldaModel.loadModel(modelPath);

        String line;
        while ((line = reader.readLine()) != null) {
            inputList.add(line.trim());
        }
        reader.close();

        ArrayList<String> rs = calTopNTopics(inputList);
        for (String s : rs) {
            writer.writeLine(s);
        }
        writer.flush();
        writer.close();
    }

    /**
     * 对输入文档集合的每一篇文档进行主题计算，输出"文档-主题"集合
     *
     * @param in
     * @return
     */
    private ArrayList<String> calTopNTopics(ArrayList<String> in) {
        ArrayList<String> rs = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (String line : in) {
            String[] items = line.split("\t");
            assert (items.length == 3);
            String uid = items[0];
            String cates = items[2];
            String[] words = items[1].trim().split(" ");
            String topicStr = outputTopic(ldaModel.inference(words), this.topN);
            sb.append(uid).append("\t").append(topicStr).append("\t")
                    .append(cates).append("\t").append(items[1]);
            rs.add(sb.toString());
            //clear
            sb.setLength(0);
        }
        return rs;
    }

    /**
     * 将输入的double数组排序后输出top n
     *
     * @param p
     * @param topN
     * @return
     */
    public String outputTopic(double[] p, int topN) {
        StringBuilder sb = new StringBuilder();
        Hashtable<Integer, Double> hash = new Hashtable<Integer, Double>();
        DecimalFormat df = new DecimalFormat("0.00000");
        for (int i = 0; i < p.length; i++) {
            hash.put(i, p[i]);
        }
        Set set = hash.entrySet();
        Map.Entry<Integer, Double>[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);
        Arrays.sort(entries, new Comparator() {
            public int compare(Object o1, Object o2) {
                double v1 = (Double) ((Map.Entry) o1).getValue();
                double v2 = (Double) ((Map.Entry) o2).getValue();
                return Double.compare(v2, v1);
            }
        });
        sb.append("[");
        for (int i = 0; i < topN && i < entries.length; i++) {
            int k = entries[i].getKey();
            double v = entries[i].getValue();
            sb.append("topic_").append(k).append("/").append(df.format(v)).append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(",")).append("]");
        return sb.toString();
    }

}
