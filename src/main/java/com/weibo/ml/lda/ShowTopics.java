package com.weibo.ml.lda;

import com.weibo.misc.AnyDoublePair;
import com.weibo.misc.Flags;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by yuanye8 on 16/9/2.
 */
public class ShowTopics implements com.weibo.tool.GenericTool {
    public void run(String[] args) throws Exception {
        Flags flags = new Flags();
        flags.add("model", "LDA model file");
        flags.add("top_n", "Number of words describe certain topic");
        flags.parseAndCheck(args);

        LdaModel model = new LdaModel();
        int top_n = flags.getInt("top_n");
        model.loadModel(flags.getString("model"));
        AnyDoublePair<Integer> [] topics = new AnyDoublePair[model.getNumTopics()];

        for (int i = 0; i < model.getNumTopics(); i++) {
            topics[i] = new AnyDoublePair<Integer>();
            topics[i].first = i;
            topics[i].second = model.pz(i);
        }

        Arrays.sort(topics, new Comparator<AnyDoublePair<Integer>>() {
            public int compare(AnyDoublePair<Integer> o1, AnyDoublePair<Integer> o2) {
                return Double.compare(o2.second, o1.second);
            }
        });

        for (int i = 0; i < topics.length; i++) {
            System.out.println("topic_" + topics[i].first + " Proportion:" + topics[i].second);
            System.out.println(model.explain(topics[i].first.intValue(), top_n));
        }
    }
}
