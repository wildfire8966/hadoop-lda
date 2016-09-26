package com.weibo.io;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * 压缩文件读取类
 * Created by yuanye8 on 16/9/26.
 */
public class GzipTextFileReader extends TextFileReader {

    public GzipTextFileReader(File file) throws IOException {
        super(file);
    }

    public GzipTextFileReader(String file) throws IOException {
        super(file);
    }

    public GzipTextFileReader(String file, String charset) throws IOException {
        super(file, charset);
    }

    public GzipTextFileReader(File file, String charset) throws IOException {
        super(file, charset);
    }

    public BufferedReader constructReader(File file, String encode) throws IOException {
        return new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(
                                new FileInputStream(file)), encode));
    }
}
