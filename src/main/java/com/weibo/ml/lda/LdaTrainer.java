package com.weibo.ml.lda;

import com.weibo.misc.Flags;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.logging.Logger;

/**
 * Created by yuanye8 on 16/9/2.
 */
public class LdaTrainer implements GenericTool {
    Logger LOG = Logger.getAnonymousLogger();
    Long startTime = 0L;

    public void run(String[] args) throws Exception {
        this.startTime = System.currentTimeMillis();
        GibbsSamplingTool sampler = new GibbsSamplingTool();
        InitModelTool initializer = new InitModelTool();

        Flags flags = new Flags();
        //必须参数
        flags.add("input", "input documents, each is space-separated words.");
        flags.add("output", "the final model, a plain text file.");
        flags.add("workingDir", "temporary directory to hold intermediate files.");
        flags.add("num_topics", "number of topics");
        flags.add("num_iterations", "number of iterations.");

        //非必须参数
        flags.addWithDefaultValue("alpha", "-1", "symmetric hyper-parameter alpha. [default k/50]");
        flags.addWithDefaultValue("beta", "0.01", "symmetric hyper-parameter beta. [default 0.01]");
        flags.addWithDefaultValue("iterations_to_keep", "10", "number of iterations to keep on disk, and used for final model. [default 10]");
        flags.addWithDefaultValue("max_num_words", "100000", "max number of words to use, sorted by TF*IDF. [default 100000]");
        flags.addWithDefaultValue("min_df", "5", "words appear in less than min_df documents will be ignored. [default 5]");
        flags.addWithDefaultValue("input_format", "text", "'sequecefile': Text value of each entry is the doc. 'text': each line is a doc. [default 'text']");

        flags.parseAndCheck(args);

        Path input = new Path(flags.getString("input"));
        Path output = new Path(flags.getString("output"));
        Path workingDir = new Path(flags.getString("workingDir"));
        int numTopics = flags.getInt("num_topics");
        int numIterations = flags.getInt("num_iterations");
        double alpha = flags.getDouble("alpha");
        if (alpha == -1.0D) {
            alpha = 50.0D / numTopics;
        }
        double beta = flags.getDouble("beta");
        int iterationToKeep = flags.getInt("iterations_to_keep");
        int maxNumWords = flags.getInt("max_num_words");
        int minDf = flags.getInt("min_df");

        JobConf conf = new JobConf();
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(workingDir)) {
            fs.mkdirs(workingDir);
        }

        Path parameters = new Path(workingDir, "parameters");
        if (!fs.exists(parameters)) {
            DataOutputStream out = fs.create(parameters, true);
            out.writeDouble(alpha);
            out.writeDouble(beta);
            out.writeInt(numTopics);
            out.close();
        }

        OutputStreamWriter likelihoodWriter = new OutputStreamWriter(
                fs.create(new Path(workingDir, "likelihood"), true), "UTF-8");

        likelihoodWriter.close();

        /**
         * 检查是否已有迭代结果，如果有，删除最新一次的结果，并准备从之前一次开始迭代
         * 每次迭代后会生成类似 docs.00001 和 nwz.00001 的文件
         */
        NumberFormat formatter = new DecimalFormat("00000");
        Path[] paths = { workingDir };
        FileStatus[] existNwz = fs.listStatus(paths, new PathFilter() {
            public boolean accept(Path p) {
                LdaTrainer.this.LOG.info("Previous data:" + p.getName());
                return p.getName().startsWith("docs.");
            }
        });
        int latest = -1;
        for (FileStatus p : existNwz) {
            int n = Integer.parseInt(p.getPath().getName().substring(5));
            if (n > latest) {
                latest = n;
            }
        }
        if (latest >= 0) {
            logAndShow("Found previous training data at iteration #" + latest + ".");
            Path latestDocs = new Path(workingDir, "docs." + formatter.format(latest));

            Path latestNwz = new Path(workingDir, "nwz." + formatter.format(latest));

            if (fs.exists(latestDocs)) {
                fs.delete(latestDocs);
            }
            if (fs.exists(latestNwz)) {
                fs.delete(latestNwz);
            }
            latest--;
            logAndShow("Remove probably incomplete iteration #"
                    + (latest + 1) + ", start with iteration #" + latest + ".");
        } else {
            logAndShow("No previous data found.");
        }

        Path docs0 = new Path(workingDir, "docs.00000");
        Path nwz0 = new Path(workingDir, "nwz.00000");
        Path tfdf = new Path(workingDir, "tfdf");
        Path words = new Path(workingDir, "words");
        int numWords = 0;
        logAndShow("Model initialized.");

        //如果不存在已有结果，进行一系列初始化操作
        // 1. 纯文本输入文件转换为
        if ((latest == -1) && (flags.getString("input_format").equals("text"))) {
            Path seqFileInput = new Path(workingDir, "input");
            PlainTextToSeqFileTool tool = new PlainTextToSeqFileTool();
            tool.convertToSequenceFile(input, seqFileInput);
            input = seqFileInput;
            logAndShow("Text input converted to SequenceFile.");
        }
        // 2.
        if (latest == -1) {
            initializer.makeWordList(input, tfdf);
        }



    }

    private void logAndShow(String s) {
        System.out.println(s);
        this.LOG.info(s);
    }

}
