/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;

/**
 *
 */
public class WordcountStreamingJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init() {
        // ClassLoader cl = ClassLoader.getSystemClassLoader();
        //
        // URL[] urls = ((URLClassLoader)cl).getURLs();
        //
        // for(URL url: urls){
        // System.out.println(url.getFile());
        // }

        String env = System.getProperty("environment");
        String test = System.getProperty("test");
        String configFile = "conf/config." + env + ".properties";

        if (test != null) {
            configFile = "config.test.properties";
        }

        System.out.println("Using this config file... [" + configFile + "]");

        try {
            props.load(ClassLoader.getSystemResourceAsStream(configFile));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file... [" + configFile + "]");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file... [" + configFile + "]");
        }

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile("hdfs:///" + parameterTool.getRequired("hdfs.wordcount.source"));

        DataStream<Tuple2<String, Integer>> transformed = text.flatMap(new Tokenizer()).keyBy(0).sum(1);

        transformed.writeAsText("hdfs:///" + parameterTool.getRequired("hdfs.wordcount.output"));

        env.execute("Wordcount Streaming");
    }

    /**
     *
     */
    public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String sentence, final Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = sentence.toLowerCase().split("\\W+");
            // System.out.println("tokens: " + Arrays.toString(tokens));
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(new String(token), new Integer(1)));
                }
            }
        }

    }
}
