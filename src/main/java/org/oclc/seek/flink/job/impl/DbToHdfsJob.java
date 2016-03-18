/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.oclc.seek.flink.function.CountRecords;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 *
 */
public class DbToHdfsJob extends JobGeneric {
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

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<Tuple2<LongWritable, DbInputRecord>> dbInputRecords =
            env.createInput(new JDBCHadoopSource().build(parameterTool))
            .map(new CountRecords<Tuple2<LongWritable, DbInputRecord>>())
            .name("db source");

        // DataStream<BaseObject> mapped =
        // resultSet
        // .map(new RichMapFunction<Tuple2<LongWritable, DbInputRecord>, BaseObject>() {
        // private static final long serialVersionUID = 1L;
        //
        // @Override
        // public BaseObject map(final Tuple2<LongWritable, DbInputRecord> tuple) throws Exception {
        // return tuple.f1.getMapped();
        // }
        // }).name("map db records");
        //
        // DataStream<String> jsonRecords = mapped.keyBy(0)
        // .countWindow(1)
        // .apply(new WindowFunction<BaseObject, String, Tuple, GlobalWindow>() {
        // private static final long serialVersionUID = 1L;
        //
        // @Override
        // public void apply(final Tuple key, final GlobalWindow window,
        // final Iterable<BaseObject> values,
        // final Collector<String> collector) throws Exception {
        //
        // BaseObject obj = values.iterator().next();
        // String jsonRecord = obj.toJson();
        //
        // collector.collect(jsonRecord);
        // }
        // }).name("transform db record into json");

        // DataStreamSink<String> filesystem =
        // String path = parameterTool.getRequired("fs.sink.dir." + suffix);
        //
        // jsonRecords.addSink(new HdfsSinkBuilder().build(path))
        // .name("put json records on filesystem");

        env.execute("Queries the DB and drops results onto Filesystem");
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
        DbToHdfsJob job = new DbToHdfsJob();
        job.init();
        job.execute(env);
    }
}
