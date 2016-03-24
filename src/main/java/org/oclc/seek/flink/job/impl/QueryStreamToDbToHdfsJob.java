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
import org.oclc.seek.flink.sink.HdfsSink;
import org.oclc.seek.flink.source.QueryLikeSource;

/**
 *
 */
public class QueryStreamToDbToHdfsJob extends JobGeneric {
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

        DataStream<String> queries = env.addSource(new QueryLikeSource()).name(QueryLikeSource.DESCRIPTION);

        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack()).name(DBFetcherCallBack.DESCRIPTION);

        DataStream<String> jsonRecords = records.map(new ObjectToJsonTransformer<EntryFind>()).name(
            ObjectToJsonTransformer.DESCRIPTION);

        String suffix = parameterTool.getRequired("db.table");
        jsonRecords.addSink(new HdfsSink(suffix, parameterTool.getProperties()).getSink()).name(HdfsSink.DESCRIPTION);

        env.execute("Receives SQL queries... executes them and then writes to hdfs");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamToDbToHdfsJob job = new QueryStreamToDbToHdfsJob();
        job.init();
        job.execute(env);
    }
}
