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
import org.oclc.seek.flink.mapper.DBFetcherCallBack;
import org.oclc.seek.flink.mapper.ObjectToJsonTransformer;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.KafkaSink;
import org.oclc.seek.flink.source.QueryLikeSource;

/**
 * Note that the Kafka source/sink is expecting the following parameters to be set <br>
 * - "bootstrap.servers" (comma separated list of kafka brokers) <br>
 * - "zookeeper.connect" (comma separated list of zookeeper servers) <br>
 * - "group.id" the id of the consumer group <br>
 * - "topic" the name of the topic to read data from.
 */
public class QueryStreamToDbToKafkaJob extends JobGeneric {
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

        DataStream<String> queries = env.addSource(new QueryLikeSource())
            .name(QueryLikeSource.DESCRIPTION);

        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack())
            .name(DBFetcherCallBack.DESCRIPTION);

        DataStream<String> jsonRecords = records.map(new ObjectToJsonTransformer<EntryFind>())
            .name(ObjectToJsonTransformer.DESCRIPTION);

        String suffix = parameterTool.getRequired("db.table");
        jsonRecords.addSink(new KafkaSink(suffix, parameterTool.getProperties()).getSink())
        .name(KafkaSink.DESCRIPTION);

        env.execute("Receives SQL queries... executes them and then writes to Kafka");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamToDbToKafkaJob job = new QueryStreamToDbToKafkaJob();
        job.init();
        job.execute(env);
    }
}
