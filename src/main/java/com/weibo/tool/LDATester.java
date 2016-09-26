package com.weibo.tool;

import com.weibo.ml.lda.ExportModelTool;
import com.weibo.ml.lda.GibbsSamplingTool;
import com.weibo.ml.lda.InitModelTool;
import com.weibo.ml.lda.PlainTextToSeqFileTool;

/**
 * 单步测试
 * Created by yuanye on 2016/9/12.
 */
public class LDATester {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: transtext initmodel sampling outputmodel");
            return;
        }

        String command = args[0];
        String[] realArgs = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            realArgs[i] = args[i + 1];
        }

        GenericTool tool = null;
        if (command.equals("transtext")) {
            tool = new PlainTextToSeqFileTool();
        } else if (command.equals("initmodel")) {
            tool = new InitModelTool();
        } else if (command.equals("sampling")) {
            tool = new GibbsSamplingTool();
        } else if (command.equals("outputmodel")) {
            tool = new ExportModelTool();
        }

        if (tool == null) {
            System.out.println("Init lda tester failed.");
        }
        tool.run(realArgs);
    }
}
