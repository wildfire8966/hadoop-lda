package com.weibo.misc;

/**
 * Created by yuanye8 on 16/9/7.
 */
public class AnyDoublePair<FirstType> {
    public FirstType first;
    public double second;

    public AnyDoublePair() {
        this.first = null;
        this.second = 0.0D;
    }

    public AnyDoublePair(FirstType first, double second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return this.first + " " + this.second;
    }
}
