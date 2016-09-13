package com.weibo.tool;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.logging.Logger;

/**
 * sequence file writer for folder.
 * Created by yuanye8 on 16/9/7.
 */
public class FolderWriter {
    Logger LOG = Logger.getAnonymousLogger();
    private SequenceFile.Writer currentWriter;

    public FolderWriter(Path path, Class key, Class value) throws IOException {
        JobConf conf = new JobConf();
        init(path, FileSystem.get(conf), conf, key, value, -1);
    }

    public FolderWriter(Path path, FileSystem fs, JobConf conf, Class key, Class value) throws IOException {
        init(path, fs, conf, key, value, -1);
    }

    public FolderWriter(Path path, FileSystem fs, JobConf conf, Class key, Class value, int part) throws IOException {
        init(path, fs, conf, key, value, part);
    }

    public void init(Path path, FileSystem fs, JobConf conf, Class key, Class value, int part)
            throws IOException {
        DecimalFormat partFormatter = new DecimalFormat("00000");
        Path outputPart = null;
        if (part >= 0) {
            outputPart = new Path(path, "part-" + partFormatter.format(part));
        } else if (fs.exists(path)) {
            Path[] root = { path };
            FileStatus[] parts = fs.listStatus(root);
            //此处很巧，parts的length就是下一个parts的标号
            String partnum = partFormatter.format(parts.length);
            outputPart = new Path(path, "part-" + partnum);
            this.LOG.info("folder exists, write to a new part:" + outputPart.toString());
        } else {
            outputPart = new Path(path, "part-00000");
            this.LOG.info("create first part");
        }

        this.currentWriter = SequenceFile.createWriter(fs, conf, outputPart, key, value);
    }

    public void append(Writable key, Writable value) throws IOException {
        this.currentWriter.append(key, value);
    }

    public void close() throws IOException {
        this.currentWriter.close();
    }
}
