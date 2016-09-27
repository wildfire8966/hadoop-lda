package com.weibo.ml.lda;

import com.weibo.misc.Flags;
import com.weibo.tool.FolderReader;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Export the accumulated NWZs from last n iterations to an independent
 * text file. Other software can then read the model in plain text file, which
 * will make the resulted model independent of Hadoop framework.
 * Created by yuanye8 on 16/9/12.
 */
public class ExportModelTool implements GenericTool {

    protected static Logger LOG = Logger.getAnonymousLogger();
    protected int[][] nwz;
    protected int numTopics;
    protected double alpha;
    protected double beta;
    protected Map<Integer, String> wordIds;

    public void run(String[] args) throws Exception {
        Flags flags = new Flags();
        flags.add("model", "Path of model directory.");
        flags.add("output", "Output model parameter file.");
        flags.add("iterations_to_user", "Use latest n iterations.");
        flags.parseAndCheck(args);

        int n = flags.getInt("iterations_to_user");
        Path modelPath = new Path(flags.getString("model"));
        Path output = new Path(flags.getString("output"));

        exportModel(modelPath, output, n);
    }

    public void exportModel(Path modelPath, Path output, int n) throws IOException {
        this.wordIds = loadWords(new Path(modelPath, "words"));
        this.nwz = new int[this.wordIds.size()][];
        loadModel(modelPath, n);
        outputModelNwz(output, n);
    }

    private void outputModelNwz(Path output, int n) throws IOException {
        FileSystem fs = FileSystem.get(new JobConf());
        OutputStreamWriter writer = new OutputStreamWriter(fs.create(output), "UTF-8");

        writer.write(this.alpha + "\n");
        writer.write(this.beta + "\n");
        writer.write(this.numTopics + "\n");
        writer.write(n + "\n");
        for (int w = 0; w < this.nwz.length; w++) {
            writer.write(this.wordIds.get(Integer.valueOf(w)));
            int[] counts = this.nwz[w];
            for (int i = 0; i < this.numTopics; i++) {
                writer.write(" ");
                writer.write(Integer.toString(counts[i]));
            }
            writer.write("\n");
        }
        writer.close();
        LOG.info("Model exported.");
    }

    private void loadModel(Path modelPath, int n) throws IOException {
        JobConf conf = new JobConf();
        FileSystem fs = FileSystem.get(conf);

        // Load model hyper-parameters.
        Path parameters = new Path(modelPath, "parameters");
        if (fs.exists(parameters)) {
            DataInputStream ins = fs.open(parameters);
            this.alpha = ins.readDouble();
            this.beta = ins.readDouble();
            this.numTopics = ins.readInt();
            ins.close();
        } else {
            //如果不存在参数文件，默认参数，便于测试
            this.numTopics = 500;
            this.alpha = 50.0 / this.numTopics;
            this.beta = 0.01;
        }

        LOG.info("Load model parameters, alpha:" + this.alpha + " beta:" + this.beta + " num_topics:" + this.numTopics);

        Path[] files = { modelPath };
        FileStatus[] modelFiles = fs.listStatus(files, new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().contains("nwz.");
            }
        });

        Arrays.sort(modelFiles, new Comparator<FileStatus>() {
            public int compare(FileStatus p0, FileStatus p1) {
                return p1.getPath().compareTo(p0.getPath());
            }
        });

        if (modelFiles.length < n) {
            n = modelFiles.length;
        }

        for (int i = 0; i < n; i++) {
            loadNwz(modelFiles[i].getPath());
            LOG.info("NWZ " + modelFiles[i].toString() + " loaded.");
        }
    }

    private void loadNwz(Path input) throws IOException {
        IntWritable word = new IntWritable();
        WordInfoWritable topicCounts = new WordInfoWritable();
        FolderReader reader = new FolderReader(input);
        while (reader.next(word, topicCounts)) {
            int[] counts = this.nwz[word.get()];
            if (counts == null) {
                counts = new int[this.numTopics];
                Arrays.fill(counts, 0);
                this.nwz[word.get()] = counts;
            }
            for (int i = 0; i < this.numTopics; i++) {
                counts[i] += topicCounts.getTopicCount(i);
            }
        }
        reader.close();
    }

    /**
     * Load word to id mapping.
     */
    private Map<Integer, String> loadWords(Path path) throws IOException {
            this.wordIds = new Hashtable<Integer, String>();
            Map<Integer, String> keymap = new Hashtable<Integer, String>();
            FolderReader reader = new FolderReader(path);
            Text key = new Text();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                keymap.put(Integer.valueOf(value.get()), key.toString());
            }
            reader.close();
        return keymap;
    }


}
