package com.weibo.io;

import java.io.*;

/**
 * 文本文件读取类
 * Created by yuanye8 on 16/9/26.
 */
public class TextFileReader {
    BufferedReader br;

    public TextFileReader(String filename) throws IOException {
        this(new File(filename), "UTF-8");
    }

    public TextFileReader(File file) throws IOException {
        this(file, "UTF-8");
    }

    public TextFileReader(File file, String encode) throws IOException {
        this.br = constructReader(file, encode);
    }

    public TextFileReader(String filename, String encode) throws IOException {
        this.br = constructReader(new File(filename), encode);
    }

    private BufferedReader constructReader(File file, String encode) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file), encode));
    }

    public String readLine() throws IOException {
        return this.br.readLine();
    }

    //一次性读取文件全部字节
    public String readAll() throws IOException {
        int bufsize = 4096;
        char[] buffer = new char[bufsize];
        int fill = 0;
        for (;;) {
            int read = this.br.read(buffer, fill, buffer.length - fill);
            if (read == -1) {
                break;
            }
            fill += read;
            //缓冲区容量已满，扩容，单位为bufsize
            if (fill >= buffer.length) {
                char[] newbuffer = new char[bufsize + buffer.length];
                for (int i = 0; i < buffer.length; i++) {
                    newbuffer[i] = buffer[i];
                }
                buffer = null;
                buffer = newbuffer;
            }
        }

        return new String(buffer, 0, fill);
    }

    public static String readAll(String filename) throws IOException {
        return readAll(filename, "UTF-8");
    }

    private static String readAll(String filename, String encode) throws IOException {
        TextFileReader reader = new TextFileReader(filename, encode);
        String result = reader.readAll();
        reader.close();
        return result;
    }

    public void close() throws IOException {
        this.br.close();
    }
}
