package com.weibo.tool;

import com.weibo.ml.lda.InferenceNewDoc;
import com.weibo.ml.lda.LdaTrainer;
import com.weibo.ml.lda.ShowTopics;

/**
 * Created by yuanye8 on 16/9/2.
 */
public class EntryPoint {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: train showModel inference");
            return;
        }

        String command = args[0];
        String[] realArgs = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            realArgs[i] = args[i + 1];
        }

        GenericTool tool = null;
        if (command.equals("train")) {
            tool = new LdaTrainer();
        } else if (command.equals("showModel")) {
            tool = new ShowTopics();
        } else if (command.equals("inference")) {
            tool = new InferenceNewDoc();
        }
        tool.run(realArgs);
    }
}
