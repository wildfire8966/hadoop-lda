package com.weibo.ml.lda;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义文档输出类：将输出转化为字节流
 * Created by yuanye8 on 16/9/6.
 */
public class DocumentWritable implements Writable{
    public int[] words = null;
    public int[] topics = null;
    private int numWords = 0;
    private byte[] buffer = new byte[10240];

    public int getNumWords() {
        return this.numWords;
    }

    public void setNumWords(int n) {
        if (this.words == null || this.words.length < n) {
            this.words = new int[n];
            this.topics = new int[n];
        }
        this.numWords = n;
    }

    /**
     * 重写写数据方法
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        int size = this.numWords * 2 * 4;
        if (this.buffer.length < size) {
            this.buffer = new byte[size + 1024];
        }
        for (int i = 0; i < this.numWords; i++) {
            intToFourBytes(this.buffer, (i * 8), this.words[i]);
            intToFourBytes(this.buffer, (i * 8 + 4), this.topics[i]);
        }
        dataOutput.writeInt(size);
        dataOutput.write(this.buffer, 0, size);
    }

    /**
     * 重写读数据方法
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        if (this.buffer.length < size) {
            this.buffer = new byte[size + 1024];
        }

        dataInput.readFully(this.buffer, 0, size);
        //字节长度转换为词个数
        setNumWords(size / 4 / 2);
        for (int i = 0; i < this.numWords; i++) {
            this.words[i] = fourBytesToInt(this.buffer, i * 8);
            this.topics[i] = fourBytesToInt(this.buffer, (i * 8 + 4));
        }
    }

    /**
     * 字节数组转化为int
     * @param b
     * @param offset
     * @return
     */
    public static int fourBytesToInt(byte[] b, int offset) {
        //与 0xff 做 & 运算会将 byte 值变成 int 类型的值, 即长度变为32位
        int i = ((b[offset] & 0xFF) << 24) + ((b[offset + 1] & 0xFF) << 16) + ((b[offset + 2] & 0xFF) << 8)
                + ((b[offset + 3] & 0xFF));
        return i;
    }

    /**
     * int转化为字节数组
     * @param b
     * @param offset
     * @param i
     */
    public static void intToFourBytes(byte[] b, int offset, int i) {
        b[offset] = (byte)(i >>> 24);
        b[offset + 1] = (byte)(i >>> 16);
        b[offset + 2] = (byte)(i >>> 8);
        b[offset + 3] = (byte)(i);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i< this.numWords; i++) {
            sb.append(i > 0 ? " ": "");
            sb.append(this.words[i]);
            sb.append(":");
            sb.append(this.topics[i]);
        }
        return sb.toString();
    }
}
