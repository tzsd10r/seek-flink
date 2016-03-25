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
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.ObjectToJsonTransformer;
import org.oclc.seek.flink.mapper.RecordCounter;
import org.oclc.seek.flink.record.BaseObject;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.sink.KafkaSink;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 *NOTE: NOT WORKING PROPERLY!!! FOR SOME REASON... DB CONNECTIONS ARE MAXING OUT.
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

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<Tuple2<LongWritable, DbInputRecord>> dbRecords =
            env.createInput(new JDBCHadoopSource(parameterTool).get())
            .name(JDBCHadoopSource.DESCRIPTION);
        
        DataStream<BaseObject> mapped = dbRecords.map(
            new RichMapFunction<Tuple2<LongWritable, DbInputRecord>, BaseObject>() {
                private static final long serialVersionUID = 1L;

                @Override
                public BaseObject map(final Tuple2<LongWritable, DbInputRecord> tuple) throws Exception {
                    return tuple.f1.getEntryFind();
                }
            }).name("Map db rows to objects");

        DataStream<String> jsonRecords = mapped.map(new ObjectToJsonTransformer<BaseObject>())
            .map(new RecordCounter<String>())
            .name(ObjectToJsonTransformer.DESCRIPTION);

        String suffix = parameterTool.getRequired("db.table");
        jsonRecords.addSink(new KafkaSink(suffix, parameterTool.getProperties()).getSink())
        .name(KafkaSink.DESCRIPTION);

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
