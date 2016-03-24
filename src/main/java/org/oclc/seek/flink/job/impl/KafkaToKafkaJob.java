/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.sink.KafkaSink;
import org.oclc.seek.flink.source.KafkaSource;

/**
 * Note that the Kafka source/sink is expecting the following parameters to be set
 * - "bootstrap.servers" (comma separated list of kafka brokers)
 * - "zookeeper.connect" (comma separated list of zookeeper servers)
 * - "group.id" the id of the consumer group
 * - "topic" the name of the topic to read data from.
 */
public class KafkaToKafkaJob extends JobGeneric {
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
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String suffix = parameterTool.getRequired("db.table");

        DataStream<String> jsonRecords =
            env.addSource(new KafkaSource(suffix, parameterTool.getProperties()).getSource())
            .name(KafkaSource.DESCRIPTION)
            .rebalance();

        // DataStream<String> enrichedJsonRecords = jsonRecords.map(new RecordCounter<String>())
        // .name(RecordCounter.DESCRIPTION);

        jsonRecords.addSink(new KafkaSink(suffix, parameterTool.getProperties()).getSink())
        .name(KafkaSink.DESCRIPTION);

        env.execute("Read Events from Kafka Source and write to Kafka Stage");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaToKafkaJob job = new KafkaToKafkaJob();
        job.init();
        job.execute(env);
    }

}
