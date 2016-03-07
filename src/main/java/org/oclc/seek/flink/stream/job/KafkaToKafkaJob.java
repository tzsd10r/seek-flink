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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;
import org.oclc.seek.flink.stream.source.KafkaSourceBuilder;

/**
 * Note that the Kafka source/sink is expecting the following parameters to be set
 * - "bootstrap.servers" (comma separated list of kafka brokers)
 * - "zookeeper.connect" (comma separated list of zookeeper servers)
 * - "group.id" the id of the consumer group
 * - "topic" the name of the topic to read data from.
 */
public class KafkaToKafkaJob extends JobGeneric implements JobContract {
    Properties props = new Properties();

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

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableSysoutLogging();
        // use system default value
        env.getConfig().setNumberOfExecutionRetries(5);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns

        /*
         * Kafka streaming source
         */
        SourceFunction<String> source =
            new KafkaSourceBuilder().build(
                parameterTool.get(parameterTool.getRequired("db.table") + ".kafka.src.topic"),
                parameterTool.getProperties());

        DataStream<String> jsonRecords = env
            .addSource(source)
            .rebalance().name("kafka source");

        jsonRecords.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String map(final String value) throws Exception {
                return parameterTool.getRequired("db.table") + ":{" + value + "}";
            }
        }).name("add root element to json record");

        jsonRecords.addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get(parameterTool.getRequired("db.table") + ".kafka.stage.topic"),
                parameterTool.getProperties()))
                .name("kafka stage");

        env.execute("Read Events from Kafka Source and write to Kafka Stage");
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
        KafkaToKafkaJob job = new KafkaToKafkaJob();
        job.init();
        job.execute(env);
    }

}
