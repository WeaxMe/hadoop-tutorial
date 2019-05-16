package com.github.weaxme;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.kerby.config.Conf;

import java.io.IOException;

@Slf4j
public class HadoopHelloWorld {

    private static final String TEST_PATH = "test.txt";
    private static final String OUTPUT_PATH = "output";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:54310");
        hdfsHelloWorld(conf);
        wordCounter(conf);
    }

    private static void hdfsHelloWorld(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        for (FileStatus status : fs.listStatus(new Path("."))) {
            log.info("status = {}", status.getPath());
        }

        Path path = new Path(TEST_PATH);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        FSDataOutputStream out = fs.create(path);
        out.writeUTF("Hello, World!\n");
        out.close();

        FSDataInputStream in = fs.open(path);
        String message = in.readUTF();
        log.info("message = {}", message);
        in.close();
    }

    private static void wordCounter(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HadoopHelloWorld.class);
        job.setMapperClass(WordCounterMapper.class);

        job.setCombinerClass(WordCounterReducer.class);
        job.setReducerClass(WordCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(TEST_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        log.info("wait for complete job: {}", job.waitForCompletion(true));
    }

}
