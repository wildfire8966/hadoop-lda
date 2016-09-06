package com.weibo.ml.lda;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yuanye on 2016/9/6.
 */
public class WordInfoWritable implements Writable {
    private int[] topicCount;
    protected byte[] buffer;
    protected  boolean isPartial;

    public WordInfoWritable(int n) {
        this.topicCount = new int[n];
        this.buffer = new byte[n * 4];
        this.isPartial = false;
    }

    public WordInfoWritable() {
        this.topicCount = null;
        this.isPartial = false;
    }

    public void setIsPartial(boolean isPartial) {
        this.isPartial = isPartial;
    }

    public int getTopicCount(int i) {
        return this.topicCount[i];
    }

    public void setTopicCount(int i, int v) {
        this.topicCount[i] = v;
    }

    public int size() {
        return this.topicCount.length;
    }

    public boolean isPartial() {
        return this.isPartial;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.topicCount.length);
        for (int i = 0; i < size(); i++) {
            DocumentWritable.intToFourBytes(this.buffer, i * 4, this.topicCount[i]);
        }
        dataOutput.write(this.buffer);
        dataOutput.writeBoolean(this.isPartial);
    }

    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        if (this.topicCount == null || (size != size())) {
            this.topicCount = new int[size];
            this.buffer = new byte[size * 4];
        }
        dataInput.readFully(this.buffer);

        for (int i = 0; i < size(); i++) {
            this.topicCount[i] = DocumentWritable.fourBytesToInt(this.buffer, i * 4);
        }
        this.isPartial = dataInput.readBoolean();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++) {
            sb.append(i);
            sb.append(":");
            sb.append(this.topicCount[i]);
            sb.append(" ");
        }
        return sb.toString();
    }
}
