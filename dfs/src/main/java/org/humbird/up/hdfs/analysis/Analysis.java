package org.humbird.up.hdfs.analysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

import static org.humbird.up.hdfs.commons.Constant.hdfcConfiguration;

/**
 * Created by david on 16/9/27.
 */
public class Analysis {

    enum Counter {
        LINESKIP,
    }

    public static class AnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                String[] lineSplit = line.split("\t");
//                String requestUrl = lineSplit[4];
//                requestUrl = requestUrl.substring(requestUrl.indexOf(' ') + 1, requestUrl.lastIndexOf(' '));
                Text out = new Text(lineSplit[4]);
                context.write(out, one);
            } catch (java.lang.ArrayIndexOutOfBoundsException e) {
                context.getCounter(Counter.LINESKIP).increment(1);
            }
        }
    }

    public static class AnalysisReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
            int count = 0;
            for (IntWritable v : values) {
                count = count + 1;
            }
            try {
                context.write(key, new IntWritable(count));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void easy() throws IOException, ClassNotFoundException, InterruptedException, YarnException {
        Job job = new Job(hdfcConfiguration);
        job.setJobName("logAnalysis");
        job.setJarByClass(Analysis.class);
        FileInputFormat.addInputPath(job, new Path("/humbird/test/1991_2016.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/humbird/test/d.txt"));
        job.setMapperClass(AnalysisMapper.class);
        job.setReducerClass(AnalysisReduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //keep the same format with the output of Map and Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
//        job.submit();

        System.out.println(job.getJobID().toString());

//        YarnClient yarnClient = YarnClient.createYarnClient();
//        yarnClient.init(hdfcConfiguration);
//        System.out.println(yarnClient.getYarnClusterMetrics().getNumNodeManagers());
//        yarnClient.getYarnClusterMetrics();
    }
}
