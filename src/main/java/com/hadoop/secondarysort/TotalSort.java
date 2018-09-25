package com.hadoop.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.File;
import java.io.IOException;


public class TotalSort {
    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        //分区文件路径
        Path partitionFile = new Path(args[2]);
        int reduceNumber = Integer.parseInt(args[3]);

        //RandomSampler第一个参数表示会被先为听概率,第二个对数是一个选取的样本数数,第三个参数是最大读取InputSplit数
        InputSampler.RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);
        Configuration conf = new Configuration();
        //设置作业的分区文件路径
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
        Job job = Job.getInstance(conf, "TotalSort");

        job.setJarByClass(TotalSort.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setNumReduceTasks(reduceNumber);

        //设置Partitioner类
        job.setPartitionerClass(TotalOrderPartitioner.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        //写入分区文件
        InputSampler.writePartitionFile(job, sampler);
        System.out.println(job.waitForCompletion(true )? 0 : 1);
        ;


    }
}
