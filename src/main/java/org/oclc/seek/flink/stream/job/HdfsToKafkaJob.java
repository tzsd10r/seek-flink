/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;

/**
 *
 */
public class HdfsToKafkaJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init() {

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls) {
            System.out.println(url.getFile());
        }

        String env = System.getProperty("environment");
        String configFile = "conf/config." + env + ".properties";

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
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // create a checkpoint every 5 secodns
        // env.enableCheckpointing(5000);

        // DataStream<String> text = env.readTextFile(parameterTool.getRequired("hdfs.kafka.source"));
        DataStream<String> text =
            env.readFileStream(parameterTool.getRequired(parameterTool.getRequired("db.table") + ".fs.src.dir"), 1000,
                WatchType.ONLY_NEW_FILES);

        text.map(new RichMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
            }

            @Override
            public String map(final String value) throws Exception {
                recordCount.add(1L);
                return value;
            }
        }).name("json-records")
        .addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get(parameterTool.getRequired("db.table") + ".kafka.stage.topic"),
                parameterTool.getProperties()))
                .name("kafka-stage");

        env.execute("Reads from HDFS and writes to Kafka Stage");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        String configFile;
        if (args.length == 0) {
            configFile = "conf/conf.prod.properties";
            System.out.println("Missing input : conf file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HdfsToKafkaJob job = new HdfsToKafkaJob();
        job.init();
        job.execute(env);
    }
}
