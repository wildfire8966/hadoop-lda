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
 * Perform Gibbs Sampling on a set of documents, according to the NWZ file.
 * First, we pass the documents to GibbsSamplingReducer by IdentityMapper. Then,
 * GibbsSamplingReducer do the sampling, output the documents with new topic
 * assignmentsm, and also output the changed NWZ file. Finally, another
 * map-reduce combines the NWZ files from different reducers into one.
 *
 * The reason of not doing sampling in the map stage is efficiency. We have to
 * load a possibly large NWZ file into memory before sampling, which may take a
 * lot of time. Normally Hadoop allocates one reducer and several mappers for
 * one machine. If we do the sampling in the map stage, the same NWZ-loading
 * work would be repeated several times on one machine, which is a waste of
 * resource and significantly slows down the whole training process.
 */
public class GibbsSamplingTool implements GenericTool {
    public  enum GibbsSamplingCounter {LIKELIHOOD};
    public static double RESOLUTION = 0.01;

    public void run(String args[]) throws IOException {
        Flags flags = new Flags();
        flags.add("input_docs");
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
        //注意此处要传入getClass()
        MapReduceJobConf job = new MapReduceJobConf(getClass());
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
        //此处除以100000是为了减小likelihood数量级，当稳定后，samping后词被分到每个主题的概率会趋于稳定likelihood的值也应该稳定下来
        double likelihood = runningJob.getCounters().getCounter(GibbsSamplingCounter.LIKELIHOOD) / RESOLUTION / 100000;
        
        combineModelParam(inputNwz, tmpNwz, outputNwz);
        fs.delete(tmpNwz);
        return likelihood;
    }

    private void combineModelParam(Path refNwz, Path inputNwz, Path outputNwz) throws IOException {
        MapReduceJobConf job = new MapReduceJobConf(getClass());
        job.setJobName("CombineModelParametersForLDA");
        SequenceFileInputFormat.addInputPath(job, inputNwz);
        SequenceFileInputFormat.addInputPath(job, refNwz);
        SequenceFileOutputFormat.setOutputPath(job, outputNwz);
        job.setMapReduce(IdentityMapper.class, CombineModelParamReducer.class);
        job.setKeyValueClass(
                IntWritable.class, WordInfoWritable.class,
                IntWritable.class, WordInfoWritable.class);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();
    }


}
