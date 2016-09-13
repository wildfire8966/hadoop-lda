package com.weibo.misc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * 工具类：计数类，用于统计词频
 * Created by yuanye8 on 16/9/6.
 */
public class Counter<KeyType> {
    private Hashtable<KeyType, Long> hash;

    public Iterator<Map.Entry<KeyType, Long>> iterator() {
        return this.hash.entrySet().iterator();
    }

    public void clear() {
        this.hash.clear();
    }

    public int size() {
        return this.hash.size();
    }

    public Counter() {
        this.hash = new Hashtable<KeyType, Long>();
    }

    public void inc(KeyType key, long delta) {
        this.hash.put(key, get(key) + delta);
    }

    public long get(KeyType key) {
        Long current = this.hash.get(key);
        if (current == null) {
            current = 0L;
        }
        return current;
    }

}
