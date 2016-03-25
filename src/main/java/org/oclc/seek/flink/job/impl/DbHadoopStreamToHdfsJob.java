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
import org.oclc.seek.flink.sink.HdfsSink;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 * Uses the HadoopInputFormat type to define the source for the database.
 * Under the hood... it uses map/reduce technology to fetch the rows from the database.
 * Additionally, the Flink environment initialized here uses the Batch API.
 * 
 * NOTE: NOT WORKING PROPERLY!!! FOR SOME REASON... DB CONNECTIONS ARE MAXING OUT.
 */
public class DbHadoopStreamToHdfsJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<Tuple2<LongWritable, DbInputRecord>> dbRows = env.createInput(
            new JDBCHadoopSource(parameterTool).get()).name(JDBCHadoopSource.DESCRIPTION);

        DataStream<BaseObject> mapped = dbRows.map(
            new RichMapFunction<Tuple2<LongWritable, DbInputRecord>, BaseObject>() {
                private static final long serialVersionUID = 1L;

                @Override
                public BaseObject map(final Tuple2<LongWritable, DbInputRecord> tuple) throws Exception {
                    return tuple.f1.getEntryFind();
                }
            }).name("Map db rows to objects");

        DataStream<String> jsonRecords = mapped.map(new ObjectToJsonTransformer<BaseObject>()).name(
            ObjectToJsonTransformer.DESCRIPTION);
        
        DataStream<String> countedRecords = jsonRecords.map(new RecordCounter<String>())
            .name(RecordCounter.DESCRIPTION);

        String suffix = parameterTool.getRequired("db.table");
        countedRecords.addSink(new HdfsSink(suffix, parameterTool.getProperties()).getSink())
            .name(HdfsSink.DESCRIPTION);

        env.execute("Queries the DB and drops results onto the Filesystem");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        System.setProperty("map.tasks", "10");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DbHadoopStreamToHdfsJob job = new DbHadoopStreamToHdfsJob();
        job.init();
        job.execute(env);
    }
}
