package com.weibo.tool;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * 文件夹读取解析工具类
 * Created by yuanye8 on 16/9/6.
 */
public class FolderReader {
    private Path path;
    private FileSystem fs;
    private JobConf conf;
    private SequenceFile.Reader currentReader;
    private FileStatus[] parts;
    private int currentPart;

    public FolderReader(Path path) throws IOException {
        JobConf conf = new JobConf();
        init(path, FileSystem.get(conf), conf);
    }

    public FolderReader(Path path, FileSystem fs, JobConf conf) throws IOException {
        init(path, fs, conf);
    }

    /**
     * 初始化读取工具类
     * @param path  读取文件夹路径
     * @param fs    HDFS文件操作系统
     * @param conf  HDFS配置
     * @throws IOException
     */
    private void init(Path path, FileSystem fs, JobConf conf)throws IOException {
        this.path = path;
        this.fs = fs;
        this.conf = conf;

        this.currentPart = -1;
        this.currentReader = null;
        Path[] paths = { path };
        this.parts = fs.listStatus(paths, new PathFilter() {
            public boolean accept(Path path) {
                return (!path.getName().startsWith(".")) && (!path.getName().startsWith("_"));
            }
        });
        nextPart();
    }

    public Class getKeyClass() {
        return this.currentReader.getKeyClass();
    }

    public Class getValueClass() {
        return this.currentReader.getValueClass();
    }

    /**
     * 每调用一次函数，将本地读取的数值赋值给传入参数
     * 1. 循环读取当前文件，将读取值赋值给传入参数（传入为引用对象而不是值）
     * 2. 当前文件读取完毕后，会循环读取下一份文件重复步骤1，直至所有文件读取完毕
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public boolean next(Writable key, Writable value) throws IOException {
        if (this.currentReader == null) {
            return false;
        }
        while (!this.currentReader.next(key, value)) {
            if (!nextPart()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reader迭代，每回返回读取下一个part的读取对象
     * @return
     * @throws IOException
     */
    private boolean nextPart() throws IOException {
        close();
        this.currentPart += 1;
        if (this.currentPart >= this.parts.length) {
            return false;
        }
        this.currentReader  = new SequenceFile.Reader(fs, parts[this.currentPart].getPath(), conf);
        return true;
    }

    public void close() throws IOException {
        if (this.currentReader != null) {
            this.currentReader.close();
            this.currentReader = null;
        }
    }
}
