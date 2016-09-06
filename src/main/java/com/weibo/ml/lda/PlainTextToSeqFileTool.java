package com.weibo.ml.lda;

import com.weibo.misc.Flags;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.io.IOException;

/**
 * 转化纯文本输入文件为hadoop二进制输入文件（专门供hadoop MR程序中间过程使用的文件格式）
 * Created by yuanye8 on 16/9/5.
 */
public class PlainTextToSeqFileTool implements GenericTool {
    public void run(String[] args) throws Exception {
        Flags flags = new Flags();
        flags.add("input");
        flags.add("output");
        flags.parseAndCheck(args);

        convertToSequenceFile(new Path(flags.getString("input")), new Path(flags.getString("output")));

    }

    public void convertToSequenceFile(Path input, Path output) throws IOException {
        JobConf job = new JobConf();

        job.setJarByClass(getClass());
        job.setJobName("text-to-sequence-file");

        job.setMapperClass(ConvertMapper.class);
        job.setReducerClass(IdentityReducer.class);
        job.setNumReduceTasks(0);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, input);
        SequenceFileOutputFormat.setOutputPath(job, output);

        JobClient.runJob(job);
    }

    public static class ConvertMapper implements Mapper<LongWritable, Text, Text, Text> {
        Text outkey = new Text();
        Text outvalue = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            this.outkey.set(Long.toString(key.get()));
            outputCollector.collect(this.outkey, value);
        }

        public void close() throws IOException {

        }

        public void configure(JobConf jobConf) {

        }
    }

}
