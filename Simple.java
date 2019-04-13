package com.simple.mr;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;


public class Simple {
    public static class MyMapper1 extends Mapper<Object,Text,LongWritable,LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long x = s.nextLong();
            long y = s.nextLong();
            context.write(new LongWritable(x),new LongWritable(y));
            s.close();
        }
    }

    public static class MyReducer1 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (LongWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(key,new LongWritable(count));
        }

    }

    public static class MyMapper2 extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable node, LongWritable count, Context context )
                throws IOException, InterruptedException {
            context.write(count,new LongWritable(1));
        }
    }

    public static class MyReducer2 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable(sum));
        }

    }

    public static void main ( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "JOB_1");
        Process OutputRemove = Runtime.getRuntime().exec("rm -r output");
        job1.setJobName("MapReduce1");
        job1.setJarByClass(Simple.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path("/Users/avinashshanker/Desktop/UTA_Masters/Sem_2_Spring_19/CSE_6331_ADB/simple/new"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "JOB_2");
        job2.setJobName("MapReduce2");
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("/Users/avinashshanker/Desktop/UTA_Masters/Sem_2_Spring_19/CSE_6331_ADB/simple/new"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);
        Process p = Runtime.getRuntime().exec("rm -r new");
    }
}


