/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.job.runner.JobRunner;
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
    }

    /**
     *
     */
    public KafkaToKafkaJob() {
        props.put("group.id", "seek-kafka");
        props.put("zookeeper.connect",
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        props.put("kafka.topic.source", "entry-find-queries");
        props.put("kafka.topic.sink", "entry-find-results");
        props.put("bootstrap.servers", "ilabhddb03dxdu:9077,ilabhddb04dxdu:9077,ilabhddb05dxdu:9077");

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableSysoutLogging();
        // use system default value
        env.getConfig().setNumberOfExecutionRetries(-1);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns

        /*
         * Kafka streaming source
         */
        SourceFunction<String> source = new KafkaSourceBuilder().build(parameterTool.getRequired("kafka.topic.source"),
            parameterTool.getProperties());

        DataStream<String> streamOfQueries = env
            .addSource(source)
            .rebalance()
            .map(new MapFunction<String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String map(final String value) throws Exception {
                    new JobRunner().run("dbimportentryfindjob");
                    return "Query: " + value + " is complete!";
                }
            }).name("entry-find-queries").broadcast();


        streamOfQueries.print().name("printing results");

        env.execute("Read Events from Kafka and write to HDFS");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        String configFile;
        if (args.length == 0) {
            configFile = "config/config.prod.properties";
            System.out.println("Missing input : config file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        KafkaToHdfsJob kh = new KafkaToHdfsJob(configFile);
        kh.execute();
    }

}
