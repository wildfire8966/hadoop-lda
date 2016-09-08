package com.weibo.ml.lda;

import com.weibo.mapred.MapReduceJobConf;
import com.weibo.misc.Flags;
import com.weibo.tool.GenericTool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.IOException;

/**
 * Created by yuanye8 on 16/9/2.
 */
public class GibbsSamplingTool implements GenericTool {
    public static double RESOLUTION = 0.01D;

    public void run(String args[]) throws IOException {
        Flags flags = new Flags();
        flags.add("intput_docs");
        flags.add("input_nwz");
        flags.add("output_docs");
        flags.add("output_nwz");
        flags.add("alpha");
        flags.add("beta");
        flags.add("num_topics");
        flags.add("num_words");
        flags.parseAndCheck(args);

        double likelihood = sampling(
                                        new Path(flags.getString("input_docs")), new Path(flags.getString("output_docs")),
                                        new Path(flags.getString("input_nwz")), new Path(flags.getString("output_nwz")),
                                        flags.getDouble("alpha"), flags.getDouble("beta"),
                                        flags.getInt("num_topics"), flags.getInt("num_words")
                                    );


        System.out.println("Done with likelihood " + likelihood);
    }

    public double sampling(Path inputDocs, Path outputDocs, Path inputNwz, Path outputNwz, double alpha, double beta, int numTopics, int numWords)
            throws IOException
    {
        JobConf envConf = new JobConf();
        FileSystem fs = FileSystem.get(envConf);

        Path tmpNwz = new Path(outputNwz + "_tmp").makeQualified(fs);
        inputNwz = inputNwz.makeQualified(fs);

        MapReduceJobConf job = new MapReduceJobConf();
        FileSystem.get(job).mkdirs(tmpNwz);
        job.setJobName("GibbsSamplingForLDA");
        job.setInputOutputPath(inputDocs, outputDocs);
        job.set("input.nwz", inputNwz.toString());
        job.set("output.nwz", tmpNwz.toString());
        job.set("alpha", Double.toString(alpha));
        job.set("beta", Double.toString(beta));
        job.set("num.topics", Integer.toString(numTopics));
        job.set("num.words", Integer.toString(numWords));
        job.setMapReduce(IdentityMapper.class, GibbsSamplingReducer.class);
        job.setKeyValueClass(Text.class, DocumentWritable.class, Text.class, DocumentWritable.class);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();
        double likelihood = runningJob.getCounters().getCounter(GibbsSamplingCounter.LIKELIHOOD) / RESOLUTION;
        
        combineModelParam(inputNwz, tmpNwz, outputNwz);
        fs.delete(tmpNwz);
        return likelihood;
    }

    private void combineModelParam(Path refNwz, Path inputNwz, Path outputNwz) throws IOException {
        MapReduceJobConf job = new MapReduceJobConf();
        job.setJobName("CombineModelParametersForLDA");
        SequenceFileInputFormat.addInputPath(job, inputNwz);
        SequenceFileInputFormat.addInputPath(job, refNwz);
        SequenceFileOutputFormat.setOutputPath(job, outputNwz);
        job.setMapReduce(IdentityMapper.class, CombineModelParamReducer.class);
        job.setKeyValueClass(IntWritable.class, WordInfoWritable.class, IntWritable.class, WordInfoWritable.class);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();
    }

    public  enum GibbsSamplingCounter {
        LIKELIHOOD
    }
}
