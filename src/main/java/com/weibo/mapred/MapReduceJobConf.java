package com.weibo.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

/**
 * 工具类：封装了MapReduce Job运行时所需的参数
 * Created by yuanye8 on 16/9/6.
 */
public class MapReduceJobConf extends JobConf {
    public MapReduceJobConf() {
        setInputFormat(SequenceFileInputFormat.class);
        setOutputFormat(SequenceFileOutputFormat.class);
        setNumMapTasks(48);
        setNumReduceTasks(40);
    }

    public MapReduceJobConf(Class jobClass) {
        super(jobClass);
        setInputFormat(SequenceFileInputFormat.class);
        setOutputFormat(SequenceFileOutputFormat.class);
        setNumMapTasks(48);
        setNumReduceTasks(40);
    }

    public void setKeyValueClass(Class<? extends WritableComparable> keyMerge,
                                 Class<? extends Writable> valueMerge,
                                 Class<? extends WritableComparable> keyOut,
                                 Class<? extends Writable> valueOut) {
        setMapOutputKeyClass(keyMerge);
        setMapOutputValueClass(valueMerge);
        setOutputKeyClass(keyOut);
        setOutputValueClass(valueOut);
    }

    public void setMapReduce(Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
        setMapperClass(mapper);
        setReducerClass(reducer);
    }

    public void setInputOutputPath(Path input, Path output) {
        SequenceFileInputFormat.addInputPath(this, input);
        SequenceFileOutputFormat.setOutputPath(this, output);
    }
}
