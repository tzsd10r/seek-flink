/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.oclc.seek.flink.job.JobGeneric;

/**
 *
 */
public class WordCountJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final ExecutionEnvironment env) throws Exception {
        // Set up the Hadoop Input Format
        Job job = Job.getInstance();

        HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable, Text>(
            new TextInputFormat(), LongWritable.class, Text.class, job);
        FileInputFormat.addInputPath(job, new Path(parameterTool.getRequired("fs.wordcount.source")));

        // 1. Create a Flink job
        // 2. Read data using the Hadoop FileInputFormat
        // - The date that is read from Hadoop InputFormats is converted into a DataSet<Tuple2<KEY,VALUE>> where
        // KEY is the key and VALUE is the value of the original Hadoop key-value pair.
        DataSet<Tuple2<LongWritable, Text>> input = env.createInput(hadoopInputFormat).name("Filesystem");

        // Use the HadoopMapFunction as wrapper around the Hadoop Mapper (Tokenizer)... and turn it into a
        // MapFunction. Tokenizes the line... and converts from Writable "Text" to String for better handling
        DataSet<Tuple2<Text, LongWritable>> tokenizedWords = input
            .flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(new TokenizerMapper()));

        // Convert Writable "Text" to String and LongWritable to Integer
        DataSet<Tuple2<String, Integer>> convertedWords = tokenizedWords.map(new ToJavaTypesMapper());

        // Group keys and aggregate counter
        DataSet<Tuple2<String, Integer>> aggregatedWords = convertedWords.groupBy(0).aggregate(Aggregations.SUM, 1);

        // Convert String back to Writable "Text" for use with Hadoop Output Format
        DataSet<Tuple2<Text, IntWritable>> hadoopResult = aggregatedWords.map(new ToHadoopTypesMapper());

        // Set up the Hadoop Output Format
        HadoopOutputFormat<Text, IntWritable> hadoopOutputFormat = new HadoopOutputFormat<Text, IntWritable>(
            new TextOutputFormat<Text, IntWritable>(), job);

        Configuration conf = hadoopOutputFormat.getConfiguration();

        // set the value for both, since this test is being executed with both types (hadoop1 and hadoop2 profile)
        conf.set("mapreduce.output.textoutputformat.separator", " : ");
        conf.set("mapred.textoutputformat.separator", " : ");

        conf.writeXml(System.out);

        // Build path to output file
        String millis = Long.toString(DateUtils.toCalendar(new Date()).getTimeInMillis());
        String filename = parameterTool.getRequired("fs.wordcount.output");

        FileOutputFormat.setOutputPath(job, new Path(filename));

        // Emit data using the Hadoop Output Format
        hadoopResult.output(hadoopOutputFormat).name("Filesystem");

        // Execute
        env.execute("WordCount Job");
    }

    /**
     *
     */
    public static class TokenizerMapper implements Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void configure(final JobConf arg0) {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void map(final LongWritable key, final Text value, final OutputCollector<Text, LongWritable> output,
            final Reporter reporter) throws IOException {
            // normalize and split the line
            String[] tokens = value.toString().toLowerCase().split("\\W+");
            // System.out.println("tokens: " + Arrays.toString(tokens));

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    output.collect(new Text(token), new LongWritable(1));
                }
            }
        }
    }

    /**
     *
     */
    public static final class ToHadoopTypesMapper extends
    RichMapFunction<Tuple2<String, Integer>, Tuple2<Text, IntWritable>> {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Text, IntWritable> map(final Tuple2<String, Integer> value) throws Exception {
            return new Tuple2<Text, IntWritable>(new Text(value.f0), new IntWritable(value.f1));
        }
    }

    /**
     *
     */
    public static final class ToJavaTypesMapper extends
    RichMapFunction<Tuple2<Text, LongWritable>, Tuple2<String, Integer>> {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> map(final Tuple2<Text, LongWritable> value) throws Exception {
            return new Tuple2<String, Integer>(value.f0.toString(), new Integer(value.f1.toString()));
        }
    }

}
