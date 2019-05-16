package com.github.weaxme;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> iterator,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable num : iterator) {
            sum += num.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
