package com.weibo.ml.lda;

import com.weibo.misc.Flags;
import com.weibo.tool.FolderReader;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * 训练模型入口
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
        flags.add("working_dir", "temporary directory to hold intermediate files.");
        flags.add("num_topics", "number of topics");
        flags.add("num_iterations", "number of iterations.");

        //非必须参数
        flags.addWithDefaultValue("alpha", "-1", "symmetric hyper-parameter alpha. [default 50.0/k]");
        flags.addWithDefaultValue("beta", "0.01", "symmetric hyper-parameter beta. [default 0.01]");
        flags.addWithDefaultValue("iterations_to_keep", "10", "number of iterations to keep on disk, and used for final model. [default 10]");
        flags.addWithDefaultValue("max_num_words", "100000", "max number of words to use, sorted by TF*IDF. [default 100000]");
        flags.addWithDefaultValue("min_df", "5", "words appear in less than min_df documents will be ignored. [default 5]");
        flags.addWithDefaultValue("input_format", "text", "'sequecefile': Text value of each entry is the doc. 'text': each line is a doc. [default 'text']");
        flags.addWithDefaultValue("map_num", "48", "overall number of map container");
        flags.addWithDefaultValue("reduce_num", "40", "overall number of reduce container");
        flags.parseAndCheck(args);

        Path input = new Path(flags.getString("input"));
        Path output = new Path(flags.getString("output"));
        Path workingDir = new Path(flags.getString("working_dir"));
        int numTopics = flags.getInt("num_topics");
        int numIterations = flags.getInt("num_iterations");
        double alpha = flags.getDouble("alpha");
        if (alpha == -1.0) {
            alpha = 50.0 / numTopics;
        }
        double beta = flags.getDouble("beta");
        int iterationToKeep = flags.getInt("iterations_to_keep");
        int maxNumWords = flags.getInt("max_num_words");
        int minDf = flags.getInt("min_df");

        int map = flags.getInt("map_num");
        int reduce = flags.getInt("reduce_num");

        JobConf conf = new JobConf();

        // Create model directory.
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(workingDir)) {
            fs.mkdirs(workingDir);
        }

        // Write model hyper-parameters to a file.
        Path parameters = new Path(workingDir, "parameters");
        if (!fs.exists(parameters)) {
            DataOutputStream out = fs.create(parameters, true);
            out.writeDouble(alpha);
            out.writeDouble(beta);
            out.writeInt(numTopics);
            out.close();
        }

        // Create likelihood file.
        OutputStreamWriter likelihoodWriter = new OutputStreamWriter(
                fs.create(new Path(workingDir, "likelihood"), true),
                "UTF-8"
        );
        likelihoodWriter.close();

        /**
         * 检查是否已有迭代结果，如果有，删除最新一次的结果，并准备从之前一次开始迭代
         * 每次迭代后会生成类似 docs.00001 和 nwz.00001 的文件
         */
        NumberFormat formatter = new DecimalFormat("00000");
        Path[] paths = { workingDir };
        FileStatus[] existNwz = fs.listStatus(paths, new PathFilter() {
            public boolean accept(Path p) {
                logAndShow("Previous data:" + p.getName());
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
        // 2. 初始化参数及模型
        if (latest == -1) {//这里一个思想很好：未初始化为-1，初始化后为0，之后递增，意义清晰
            initializer.makeWordList(input, tfdf, map, reduce);
            numWords = initializer.selectWords(tfdf, words, maxNumWords, minDf);
            initializer.initModel(input, docs0, nwz0, words, numTopics, numWords, map, reduce);
            latest = 0;
            logAndShow("Doc initialized.");
        } else {
            numWords = loadNumWords(words);
        }
        //3. 开始迭代
        for (int i = latest; i < numIterations; i++) {
            logAndShow("Begin iteration #" + (i + 1));
            Path previousDocs = new Path(workingDir, "docs." + formatter.format(i));
            Path previousNwz = new Path(workingDir, "nwz." + formatter.format(i));
            Path targetDocs = new Path(workingDir, "docs." + formatter.format(i + 1));
            Path targetNwz = new Path(workingDir, "nwz." + formatter.format(i + 1));
            double likelihood = sampler.sampling(
                    previousDocs, targetDocs,
                    previousNwz, targetNwz,
                    alpha, beta, numTopics, numWords,
                    map, reduce);
            logAndShow("#" + (i + 1) + " Likelihood: " + likelihood);
            likelihoodWriter = new OutputStreamWriter(
                    fs.create(new Path(workingDir, "likelihood"), true),
                    "UTF-8");
            likelihoodWriter.append(Double.toString(likelihood));
            likelihoodWriter.append("\n");
            likelihoodWriter.close();

            //只保留最近n次
            if (i + 1 - iterationToKeep >= 0) {
                Path oldDocs = new Path(workingDir, "docs." + formatter.format(i + 1 - iterationToKeep));
                fs.delete(oldDocs);
                Path oldNwz = new Path(workingDir, "nwz." + formatter.format(i + 1 - iterationToKeep));
                fs.delete(oldNwz);
            }
        }

        //output tassign matrix to text file
        Path targetDocs = new Path(workingDir, "docs." + formatter.format(numIterations));
        combineDocs(targetDocs);

        likelihoodWriter.close();
        logAndShow("Training done.");

        logAndShow("Export model to model file.");
        ExportModelTool exportModelTool = new ExportModelTool();
        exportModelTool.exportModel(workingDir, output, iterationToKeep);
        logAndShow("Model exported.");

        long endTime = System.currentTimeMillis();
        DecimalFormat decimalFormat = new DecimalFormat("0.0");
        String duration_hour = decimalFormat.format((endTime - this.startTime) / 3600.0 / 1000.0);
        logAndShow("Training time consuming: " + duration_hour + " hours.");
    }

    private void combineDocs(Path originalDocs) throws IOException {
        JobConf job = new JobConf();
        job.setJarByClass(getClass());
        job.setJobName("OutputTassignMatrixForLDA");
        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(job);
        ContentSummary sumary = fs.getContentSummary(originalDocs);
        //each reducer deal with data less than 2G
        int reducerNumer = (int)Math.round(sumary.getLength() / 1024.0 / 1024.0 / 1024.0 / 2.0);
        if (reducerNumer == 0) {
            reducerNumer = 1;
        }
        job.setNumReduceTasks(reducerNumer);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocumentWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(IdentityMapper.class);
        job.setReducerClass(CombineTassignReducer.class);

        SequenceFileInputFormat.addInputPath(job, originalDocs);
        FileOutputFormat.setOutputPath(job, new Path(originalDocs.getParent(), "tassign"));

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();
    }

    private int loadNumWords(Path words) throws IOException {
        FolderReader reader = new FolderReader(words);
        int numWords = 0;
        Text key = new Text();
        IntWritable value = new IntWritable();
        while (reader.next(key, value)) {
            numWords++;
        }
        reader.close();
        return numWords;
    }

    private void logAndShow(String s) {
        System.out.println(s);
        this.LOG.info(s);
    }

}
