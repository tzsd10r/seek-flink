/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.io.Serializable;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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
public class KafkaToKafkaJob extends JobGeneric implements JobContract, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // defines how many times the job is restarted after a failure
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 60000));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns

        final String prefix = parameterTool.getRequired("db.table");

        /*
         * Kafka streaming source
         */
        SourceFunction<String> source =
            new KafkaSourceBuilder().build(
                parameterTool.get(prefix + ".kafka.src.topic"),
                parameterTool.getProperties());

        DataStream<String> jsonRecords = env
            .addSource(source).name("kafka source")
            .rebalance();

        DataStream<String> enrichedJsonRecords = jsonRecords.map(new RichMapFunction<String, String>() {
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
                return prefix + ":{" + value + "}";
            }
        }).name("add root element to json record");

        DataStreamSink<String> kafka = enrichedJsonRecords.addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get(prefix + ".kafka.stage.topic"),
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
