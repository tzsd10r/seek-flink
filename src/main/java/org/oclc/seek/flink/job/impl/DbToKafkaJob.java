/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.oclc.seek.flink.function.CountRecords;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.sink.KafkaSinkBuilder;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 *
 */
public class DbToKafkaJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // create a checkpoint every 1000 ms
        // env.enableCheckpointing(1000);

        // set the timeout low to minimize latency
        // env.setBufferTimeout(10);

        // defines how many times the job is restarted after a failure
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 60000));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        final String suffix = parameterTool.getRequired("db.table");

        DataStream<Tuple2<LongWritable, DbInputRecord>> rawRecords =
            env.createInput(new JDBCHadoopSource().build(parameterTool))
            .map(new CountRecords<Tuple2<LongWritable, DbInputRecord>>())
            .name("db source");

        DataStream<String> jsonRecords =
            rawRecords
            .map(new RichMapFunction<Tuple2<LongWritable, DbInputRecord>, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String map(final Tuple2<LongWritable, DbInputRecord> tuple) throws Exception {
                    return tuple.f1.toJson();
                }
            }).name("convert db record into json");

        jsonRecords.addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get("kafka.sink.topic." + suffix),
                parameterTool.getProperties()))
                .name("put json records on Kafka");

        env.execute("Queries the DB and drops results onto Kafka");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DbToKafkaJob job = new DbToKafkaJob();
        job.init();
        job.execute(env);
    }
}
