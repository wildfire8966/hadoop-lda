package com.weibo.io;

import java.io.*;

/**
 * Created by yuanye8 on 16/9/28.
 */
public class TextFileWriter {
    BufferedWriter osw;

    public TextFileWriter(String filename) throws IOException {
        this(new File(filename), "UTF-8", false);
    }

    public TextFileWriter(String filename, boolean append) throws IOException {
        this(new File(filename), "UTF-8", append);
    }

    public TextFileWriter(File file) throws IOException {
        this(file, "UTF-8", false);
    }

    public TextFileWriter(String filename, String charset) throws IOException {
        this(new File(filename), charset, false);
    }

    public TextFileWriter(String filename, String charset, boolean append)
            throws IOException {
        this(new File(filename), charset, append);
    }

    public TextFileWriter(File file, String charset, boolean append) throws IOException {
        osw = constructWriter(file, charset, append);
    }

    private BufferedWriter constructWriter(File file, String charset, boolean append)
            throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(file, append), "UTF-8"));
    }

    public void writeLine(String str) throws IOException {
        this.osw.write(str);
        this.osw.write("\n");
    }

    public void write(String str) throws IOException {
        this.osw.write(str);
    }

    public void flush() throws IOException {
        osw.flush();
    }

    public void close() throws IOException {
        osw.close();
    }

    public void append(CharSequence cs) throws IOException {
        osw.append(cs);
    }
}
